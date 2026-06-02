// ==================== CLOUDFLARE WORKER + DURABLE OBJECT ====================
// Multiplayer server for Board Games using WebSocket Hibernation API
// Supports: Connect 4, Tic Tac Toe, Dots & Boxes

// Game types
const GAMES = {
    CONNECT4: 'connect4',
    TICTACTOE: 'tictactoe',
    DOTSBOXES: 'dotsboxes'
};

// Room code generation
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

function generateRoomCode() {
    let code = '';
    for (let i = 0; i < 6; i++) {
        code += CODE_CHARS.charAt(Math.floor(Math.random() * CODE_CHARS.length));
    }
    return code;
}

function generatePlayerId() {
    return crypto.randomUUID();
}

// ==================== DURABLE OBJECT: GAME ROOM ====================
export class GameRoom extends DurableObject {
    constructor(ctx, env) {
        super(ctx, env);
        this.ctx = ctx;
        this.env = env;

        // Restore any existing WebSocket sessions after hibernation
        this.sessions = new Map(); // ws -> { playerId, playerNumber, roomCode, playerName, disconnected }
        this.rooms = new Map();    // roomCode -> room data (persisted in storage)

        // Restore existing websockets
        const websockets = this.ctx.getWebSockets();
        for (const ws of websockets) {
            const attachment = ws.deserializeAttachment();
            if (attachment) {
                this.sessions.set(ws, attachment);
            }
        }
    }

    async fetch(request) {
        const url = new URL(request.url);

        // Handle WebSocket upgrade
        const upgradeHeader = request.headers.get('Upgrade');
        if (upgradeHeader === 'websocket') {
            const webSocketPair = new WebSocketPair();
            const [client, server] = Object.values(webSocketPair);

            // Accept with hibernation API
            this.ctx.acceptWebSocket(server);

            // Store minimal session data
            server.serializeAttachment({
                playerId: null,
                playerNumber: null,
                roomCode: null,
                playerName: null,
                disconnected: false
            });

            this.sessions.set(server, {
                playerId: null,
                playerNumber: null,
                roomCode: null,
                playerName: null,
                disconnected: false
            });

            return new Response(null, {
                status: 101,
                webSocket: client
            });
        }

        return new Response('Expected WebSocket', { status: 400 });
    }

    async webSocketMessage(ws, message) {
        try {
            const data = JSON.parse(message);
            await this.handleMessage(ws, data);
        } catch (e) {
            console.error('Invalid message:', e);
            this.send(ws, { type: 'error', error: 'Invalid message format' });
        }
    }

    async webSocketClose(ws, code, reason, wasClean) {
        const session = this.sessions.get(ws);
        if (session && session.roomCode) {
            await this.handleDisconnect(ws, session);
        }
        this.sessions.delete(ws);
    }

    async webSocketError(ws, error) {
        console.error('WebSocket error:', error);
        const session = this.sessions.get(ws);
        if (session && session.roomCode) {
            await this.handleDisconnect(ws, session);
        }
        this.sessions.delete(ws);
    }

    // ==================== MESSAGE HANDLERS ====================

    async handleMessage(ws, message) {
        const { type, data } = message;

        switch (type) {
            case 'createRoom':
                await this.handleCreateRoom(ws, data);
                break;
            case 'joinRoom':
                await this.handleJoinRoom(ws, data);
                break;
            case 'selectGame':
                await this.handleSelectGame(ws, data);
                break;
            case 'makeMove':
                await this.handleMakeMove(ws, data);
                break;
            case 'reconnect':
                await this.handleReconnect(ws, data);
                break;
            case 'leaveRoom':
                await this.handleLeaveRoom(ws);
                break;
            case 'ping':
                this.send(ws, { type: 'pong' });
                break;
            default:
                this.send(ws, { type: 'error', error: 'Unknown message type: ' + type });
        }
    }

    async handleCreateRoom(ws, data) {
        const { playerName } = data;

        // Generate unique room code
        let roomCode;
        let attempts = 0;
        const existingRooms = await this.ctx.storage.list({ prefix: 'room:' });
        const roomCodes = new Set();
        for (const [key] of existingRooms) {
            roomCodes.add(key.replace('room:', ''));
        }

        do {
            roomCode = generateRoomCode();
            attempts++;
        } while (roomCodes.has(roomCode) && attempts < 100);

        if (attempts >= 100) {
            this.send(ws, { type: 'error', error: 'Failed to generate room code. Please try again.' });
            return;
        }

        const playerId = generatePlayerId();
        const roomKey = 'room:' + roomCode;

        // Room created WITHOUT gameType — game selection happens after both players connect
        const room = {
            code: roomCode,
            gameType: null,           // CHANGED: null initially, set later by host
            hostId: playerId,
            hostWs: ws,               // Store reference for broadcasting
            guestId: null,
            guestWs: null,
            hostName: playerName || 'Host',
            guestName: null,
            gameState: null,
            currentPlayer: 1,
            status: 'waiting',        // waiting -> selecting -> playing -> ended
            createdAt: Date.now(),
            lastActivity: Date.now(),
            moveHistory: [],
            disconnectedPlayers: new Map(),
            hostSelectedGame: false   // Track if host has made selection
        };

        // Store in Durable Object storage (survives hibernation)
        await this.ctx.storage.put(roomKey, {
            code: roomCode,
            gameType: null,
            hostId: playerId,
            guestId: null,
            hostName: playerName || 'Host',
            guestName: null,
            gameState: null,
            currentPlayer: 1,
            status: 'waiting',
            createdAt: Date.now(),
                                   lastActivity: Date.now(),
                                   moveHistory: [],
                                   disconnectedPlayers: {},
                                   hostSelectedGame: false
        });

        // Update session
        const session = {
            playerId: playerId,
            playerNumber: 1,
            roomCode: roomCode,
            playerName: playerName || 'Host',
            disconnected: false
        };
        this.sessions.set(ws, session);
        ws.serializeAttachment(session);

        // Notify host
        this.send(ws, {
            type: 'roomCreated',
            roomCode: roomCode,
            playerId: playerId,
            playerNumber: 1,
            message: 'Room created! Share this code with your opponent.'
        });

        // Store in-memory room reference for active sessions
        this.rooms.set(roomCode, room);
    }

    async handleJoinRoom(ws, data) {
        const { roomCode, playerName } = data;
        const normalizedCode = roomCode.toUpperCase().trim();
        const roomKey = 'room:' + normalizedCode;

        // Load room from storage
        const roomData = await this.ctx.storage.get(roomKey);
        if (!roomData) {
            this.send(ws, { type: 'error', error: 'Room not found. Check the code and try again.' });
            return;
        }

        // Check room status
        if (roomData.status !== 'waiting') {
            this.send(ws, { type: 'error', error: 'Room is full or game already in progress.' });
            return;
        }
        if (roomData.guestId) {
            this.send(ws, { type: 'error', error: 'Room is already full.' });
            return;
        }

        const playerId = generatePlayerId();

        // Update room data in storage
        roomData.guestId = playerId;
        roomData.guestName = playerName || 'Guest';
        roomData.status = 'selecting';     // CHANGED: 'selecting' instead of 'playing'
        roomData.lastActivity = Date.now();
        await this.ctx.storage.put(roomKey, roomData);

        // Update in-memory room
        let room = this.rooms.get(normalizedCode);
        if (!room) {
            room = {
                code: roomData.code,
                gameType: null,
                hostId: roomData.hostId,
                guestId: playerId,
                hostName: roomData.hostName,
                guestName: playerName || 'Guest',
                gameState: null,
                currentPlayer: 1,
                status: 'selecting',
                createdAt: roomData.createdAt,
                lastActivity: Date.now(),
                moveHistory: [],
                disconnectedPlayers: new Map(),
                hostSelectedGame: false
            };
            this.rooms.set(normalizedCode, room);
        } else {
            room.guestId = playerId;
            room.guestName = playerName || 'Guest';
            room.status = 'selecting';
            room.guestWs = ws;
        }

        // Update guest session
        const session = {
            playerId: playerId,
            playerNumber: 2,
            roomCode: normalizedCode,
            playerName: playerName || 'Guest',
            disconnected: false
        };
        this.sessions.set(ws, session);
        ws.serializeAttachment(session);

        // Find host websocket
        const hostWs = this.findHostWs(normalizedCode, roomData.hostId);

        // Notify guest they joined
        this.send(ws, {
            type: 'joinedRoom',
            roomCode: normalizedCode,
            playerId: playerId,
            playerNumber: 2,
            gameType: null,              // CHANGED: null, game not selected yet
            currentPlayer: 1,
            message: 'Joined room! Waiting for host to select a game...'
        });

        // Notify host that guest joined — BOTH now see game selection
        if (hostWs) {
            this.send(hostWs, {
                type: 'opponentJoined',
                playerName: playerName || 'Guest',
                playerNumber: 2,
                currentPlayer: 1,
                message: 'Opponent joined! Select a game to start.'
            });
        }

        // CHANGED: Do NOT start game automatically. Instead, notify BOTH to show game selection
        this.broadcastToRoom(normalizedCode, {
            type: 'showGameSelection',
            message: 'Both players connected. Host: please select a game.'
        }, ws);
    }

    async handleSelectGame(ws, data) {
        const session = this.sessions.get(ws);
        if (!session || session.playerNumber !== 1) {
            this.send(ws, { type: 'error', error: 'Only the host can select the game.' });
            return;
        }

        const { gameType } = data;
        const roomCode = session.roomCode;
        const roomKey = 'room:' + roomCode;

        // Validate game type
        const validGames = ['connect4', 'tictactoe', 'dotsboxes'];
        if (!validGames.includes(gameType)) {
            this.send(ws, { type: 'error', error: 'Invalid game type.' });
            return;
        }

        // Load and update room
        const roomData = await this.ctx.storage.get(roomKey);
        if (!roomData || roomData.status !== 'selecting') {
            this.send(ws, { type: 'error', error: 'Room not available for game selection.' });
            return;
        }

        roomData.gameType = gameType;
        roomData.status = 'playing';
        roomData.lastActivity = Date.now();
        roomData.hostSelectedGame = true;

        // Initialize game state based on type
        roomData.gameState = this.initializeGameState(gameType);
        roomData.currentPlayer = Math.random() > 0.5 ? 1 : 2;

        await this.ctx.storage.put(roomKey, roomData);

        // Update in-memory room
        const room = this.rooms.get(roomCode);
        if (room) {
            room.gameType = gameType;
            room.status = 'playing';
            room.gameState = JSON.parse(JSON.stringify(roomData.gameState));
            room.currentPlayer = roomData.currentPlayer;
        }

        // Broadcast game start to ALL players in room
        this.broadcastToRoom(roomCode, {
            type: 'gameStart',
            gameType: gameType,
            currentPlayer: roomData.currentPlayer,
            gameState: roomData.gameState,
            message: 'Game starting! ' + (roomData.currentPlayer === 1 ? roomData.hostName : roomData.guestName) + ' goes first.'
        });
    }

    async handleMakeMove(ws, data) {
        const session = this.sessions.get(ws);
        if (!session || !session.roomCode) {
            this.send(ws, { type: 'error', error: 'Not in a room.' });
            return;
        }

        const roomCode = session.roomCode;
        const roomKey = 'room:' + roomCode;
        const roomData = await this.ctx.storage.get(roomKey);

        if (!roomData || roomData.status !== 'playing') {
            this.send(ws, { type: 'error', error: 'Game not active.' });
            return;
        }

        // Validate turn
        if (roomData.currentPlayer !== session.playerNumber) {
            this.send(ws, { type: 'error', error: 'Not your turn.' });
            return;
        }

        // Validate and apply move
        const validation = this.validateMove(roomData, session.playerNumber, data);
        if (!validation.valid) {
            this.send(ws, { type: 'error', error: validation.error });
            return;
        }

        const result = this.applyMove(roomData, session.playerNumber, data);
        if (result.error) {
            this.send(ws, { type: 'error', error: result.error });
            return;
        }

        // Update move history and activity
        roomData.moveHistory.push({
            player: session.playerNumber,
            move: data,
            timestamp: Date.now()
        });
        roomData.lastActivity = Date.now();

        // Save updated state
        await this.ctx.storage.put(roomKey, roomData);

        // Update in-memory room
        const room = this.rooms.get(roomCode);
        if (room) {
            room.gameState = JSON.parse(JSON.stringify(roomData.gameState));
            room.currentPlayer = roomData.currentPlayer;
        }

        // Broadcast move to all players
        this.broadcastToRoom(roomCode, {
            type: 'moveMade',
            player: session.playerNumber,
            move: data,
            gameState: roomData.gameState,
            currentPlayer: roomData.currentPlayer,
            result: result
        });

        // Handle game end
        if (result.win || result.draw) {
            roomData.status = 'ended';
            await this.ctx.storage.put(roomKey, roomData);

            this.broadcastToRoom(roomCode, {
                type: 'gameEnd',
                winner: result.winner,
                draw: result.draw,
                scores: result.scores,
                winCells: result.winCells
            });
        }
    }

    async handleReconnect(ws, data) {
        const { playerId, roomCode } = data;
        const normalizedCode = roomCode.toUpperCase().trim();
        const roomKey = 'room:' + normalizedCode;

        const roomData = await this.ctx.storage.get(roomKey);
        if (!roomData) {
            this.send(ws, { type: 'error', error: 'Room no longer exists.' });
            return;
        }

        // Check if this player was in this room
        const isHost = roomData.hostId === playerId;
        const isGuest = roomData.guestId === playerId;

        if (!isHost && !isGuest) {
            this.send(ws, { type: 'error', error: 'Invalid reconnection.' });
            return;
        }

        const playerNumber = isHost ? 1 : 2;
        const playerName = isHost ? roomData.hostName : roomData.guestName;

        // Update session
        const session = {
            playerId: playerId,
            playerNumber: playerNumber,
            roomCode: normalizedCode,
            playerName: playerName,
            disconnected: false
        };
        this.sessions.set(ws, session);
        ws.serializeAttachment(session);

        // Update in-memory room references
        let room = this.rooms.get(normalizedCode);
        if (!room) {
            room = {
                code: roomData.code,
                gameType: roomData.gameType,
                hostId: roomData.hostId,
                guestId: roomData.guestId,
                hostName: roomData.hostName,
                guestName: roomData.guestName,
                gameState: roomData.gameState,
                currentPlayer: roomData.currentPlayer,
                status: roomData.status,
                createdAt: roomData.createdAt,
                lastActivity: Date.now(),
                moveHistory: roomData.moveHistory,
                disconnectedPlayers: new Map(),
                hostSelectedGame: roomData.hostSelectedGame
            };
            this.rooms.set(normalizedCode, room);
        }

        if (isHost) room.hostWs = ws;
        else room.guestWs = ws;

        // Remove from disconnected tracking
        if (room.disconnectedPlayers && room.disconnectedPlayers.has(playerId)) {
            room.disconnectedPlayers.delete(playerId);
        }
        if (roomData.disconnectedPlayers && roomData.disconnectedPlayers[playerId]) {
            delete roomData.disconnectedPlayers[playerId];
            await this.ctx.storage.put(roomKey, roomData);
        }

        // Notify reconnected player
        this.send(ws, {
            type: 'reconnected',
            roomCode: normalizedCode,
            playerNumber: playerNumber,
            gameState: roomData.gameState,
            currentPlayer: roomData.currentPlayer,
            gameType: roomData.gameType,
            status: roomData.status,
            message: 'Reconnected successfully!'
        });

        // Notify opponent
        const opponentWs = isHost ? room.guestWs : room.hostWs;
        if (opponentWs && this.sessions.has(opponentWs)) {
            this.send(opponentWs, {
                type: 'opponentReconnected',
                message: 'Opponent reconnected!'
            });
        }

        // If room was in 'waiting_reconnect' status, resume
        if (roomData.status === 'waiting_reconnect') {
            roomData.status = roomData.gameType ? 'playing' : 'selecting';
            roomData.lastActivity = Date.now();
            await this.ctx.storage.put(roomKey, roomData);
            room.status = roomData.status;
        }
    }

    async handleDisconnect(ws, session) {
        if (!session || !session.roomCode) return;

        const roomCode = session.roomCode;
        const roomKey = 'room:' + roomCode;

        const roomData = await this.ctx.storage.get(roomKey);
        if (!roomData) return;

        // Mark player as disconnected in storage
        if (!roomData.disconnectedPlayers) roomData.disconnectedPlayers = {};
        roomData.disconnectedPlayers[session.playerId] = {
            playerNumber: session.playerNumber,
            disconnectTime: Date.now(),
            gameState: roomData.gameState,
            currentPlayer: roomData.currentPlayer
        };

        // If game hasn't started yet (waiting/selecting), just clean up
        if (roomData.status === 'waiting' || roomData.status === 'selecting') {
            await this.ctx.storage.delete(roomKey);
            this.rooms.delete(roomCode);

            // Notify other player if they exist
            const otherWs = session.playerNumber === 1 ?
            this.findGuestWs(roomCode, roomData.guestId) :
            this.findHostWs(roomCode, roomData.hostId);
            if (otherWs) {
                this.send(otherWs, {
                    type: 'opponentLeft',
                    message: 'Opponent left before the game started.'
                });
            }
            return;
        }

        // For active games, set reconnection window
        roomData.status = 'waiting_reconnect';
        await this.ctx.storage.put(roomKey, roomData);

        const room = this.rooms.get(roomCode);
        if (room) {
            room.status = 'waiting_reconnect';
            if (session.playerNumber === 1) room.hostWs = null;
            else room.guestWs = null;
        }

        // Notify opponent
        const opponentWs = session.playerNumber === 1 ?
        this.findGuestWs(roomCode, roomData.guestId) :
        this.findHostWs(roomCode, roomData.hostId);

        if (opponentWs) {
            this.send(opponentWs, {
                type: 'opponentDisconnected',
                message: 'Opponent disconnected. Waiting for reconnection...',
                reconnectWindow: 60000
            });
        }

        // Set alarm for cleanup after 60 seconds
        await this.ctx.storage.setAlarm(Date.now() + 60000);
    }

    async handleLeaveRoom(ws) {
        const session = this.sessions.get(ws);
        if (session) {
            await this.handleDisconnect(ws, session);
        }
    }

    // ==================== ALARM HANDLER (Reconnection timeout) ====================
    async alarm() {
        // Check all rooms for expired reconnection windows
        const allRooms = await this.ctx.storage.list({ prefix: 'room:' });
        const now = Date.now();

        for (const [key, roomData] of allRooms) {
            if (roomData.status !== 'waiting_reconnect') continue;

            // Check if any disconnected players have exceeded timeout
            let expired = false;
            for (const playerId in roomData.disconnectedPlayers) {
                const info = roomData.disconnectedPlayers[playerId];
                if (now - info.disconnectTime > 60000) {
                    expired = true;
                    break;
                }
            }

            if (expired) {
                // Notify remaining player and clean up
                const roomCode = roomData.code;
                const room = this.rooms.get(roomCode);

                // Find connected player
                let connectedWs = null;
                for (const [ws, session] of this.sessions) {
                    if (session.roomCode === roomCode && !session.disconnected) {
                        connectedWs = ws;
                        break;
                    }
                }

                if (connectedWs) {
                    this.send(connectedWs, {
                        type: 'opponentLeft',
                        message: 'Opponent did not reconnect. Match ended.'
                    });
                }

                await this.ctx.storage.delete(key);
                this.rooms.delete(roomCode);
            }
        }
    }

    // ==================== HELPERS ====================

    send(ws, message) {
        if (ws && ws.readyState === WebSocket.OPEN) {
            try {
                ws.send(JSON.stringify(message));
            } catch (e) {
                console.error('Send failed:', e);
            }
        }
    }

    broadcastToRoom(roomCode, message, excludeWs = null) {
        for (const [ws, session] of this.sessions) {
            if (session.roomCode === roomCode && ws !== excludeWs && ws.readyState === WebSocket.OPEN) {
                this.send(ws, message);
            }
        }
    }

    findHostWs(roomCode, hostId) {
        for (const [ws, session] of this.sessions) {
            if (session.roomCode === roomCode && session.playerId === hostId) {
                return ws;
            }
        }
        return null;
    }

    findGuestWs(roomCode, guestId) {
        for (const [ws, session] of this.sessions) {
            if (session.roomCode === roomCode && session.playerId === guestId) {
                return ws;
            }
        }
        return null;
    }

    // ==================== GAME LOGIC ====================

    initializeGameState(gameType) {
        switch (gameType) {
            case 'connect4':
                return { board: Array(6).fill(null).map(() => Array(7).fill(0)) };
            case 'tictactoe':
                return { board: Array(9).fill(null), history: { X: [], O: [] }, phase: 'place' };
            case 'dotsboxes':
                return {
                    hLines: Array(5).fill(null).map(() => Array(4).fill(0)),
                    vLines: Array(4).fill(null).map(() => Array(5).fill(0)),
                    boxes: Array(4).fill(null).map(() => Array(4).fill(0))
                };
            default:
                return null;
        }
    }

    validateMove(roomData, playerNumber, moveData) {
        if (roomData.currentPlayer !== playerNumber) {
            return { valid: false, error: 'Not your turn' };
        }
        if (roomData.status !== 'playing') {
            return { valid: false, error: 'Game not active' };
        }

        switch (roomData.gameType) {
            case 'connect4':
                return this.validateConnect4Move(roomData, moveData);
            case 'tictactoe':
                return this.validateTicTacToeMove(roomData, moveData);
            case 'dotsboxes':
                return this.validateDotsBoxesMove(roomData, moveData);
            default:
                return { valid: false, error: 'Unknown game type' };
        }
    }

    validateConnect4Move(roomData, moveData) {
        const { column } = moveData;
        if (typeof column !== 'number' || column < 0 || column > 6) {
            return { valid: false, error: 'Invalid column' };
        }
        const board = roomData.gameState.board;
        if (board[0][column] !== 0) {
            return { valid: false, error: 'Column is full' };
        }
        return { valid: true };
    }

    validateTicTacToeMove(roomData, moveData) {
        const { index, phase } = moveData;
        if (typeof index !== 'number' || index < 0 || index > 8) {
            return { valid: false, error: 'Invalid position' };
        }
        const board = roomData.gameState.board;
        const player = roomData.currentPlayer === 1 ? 'X' : 'O';
        const history = roomData.gameState.history;

        if (phase === 'place') {
            if (board[index] !== null) {
                return { valid: false, error: 'Position already occupied' };
            }
            if (history[player].length >= 3) {
                return { valid: false, error: 'All pieces placed' };
            }
        } else {
            if (!moveData.selectedPiece && board[index] !== player) {
                return { valid: false, error: 'Select your piece first' };
            }
            if (moveData.selectedPiece && board[index] !== null) {
                return { valid: false, error: 'Destination occupied' };
            }
        }
        return { valid: true };
    }

    validateDotsBoxesMove(roomData, moveData) {
        const { r, c, isHorizontal } = moveData;
        if (typeof r !== 'number' || typeof c !== 'number' || typeof isHorizontal !== 'boolean') {
            return { valid: false, error: 'Invalid move data' };
        }
        const lines = isHorizontal ? roomData.gameState.hLines : roomData.gameState.vLines;
        if (lines[r][c] !== 0) {
            return { valid: false, error: 'Line already drawn' };
        }
        return { valid: true };
    }

    applyMove(roomData, playerNumber, moveData) {
        switch (roomData.gameType) {
            case 'connect4':
                return this.applyConnect4Move(roomData, playerNumber, moveData);
            case 'tictactoe':
                return this.applyTicTacToeMove(roomData, playerNumber, moveData);
            case 'dotsboxes':
                return this.applyDotsBoxesMove(roomData, playerNumber, moveData);
        }
    }

    applyConnect4Move(roomData, playerNumber, moveData) {
        const { column } = moveData;
        const board = roomData.gameState.board;
        let row = 5;
        while (row >= 0 && board[row][column] !== 0) row--;

        if (row >= 0) {
            board[row][column] = playerNumber;
            const winResult = this.checkConnect4Win(board, row, column, playerNumber);
            if (winResult) {
                return { win: true, winner: playerNumber, winCells: winResult };
            }
            if (board[0].every(cell => cell !== 0)) {
                return { draw: true };
            }
            roomData.currentPlayer = roomData.currentPlayer === 1 ? 2 : 1;
            return { success: true };
        }
        return { error: 'Invalid move' };
    }

    checkConnect4Win(board, row, col, player) {
        const directions = [[0, 1], [1, 0], [1, 1], [1, -1]];
        for (const [dr, dc] of directions) {
            const cells = [[row, col]];
            for (let i = 1; i < 4; i++) {
                const r = row + dr * i, c = col + dc * i;
                if (r < 0 || r > 5 || c < 0 || c > 6 || board[r][c] !== player) break;
                cells.push([r, c]);
            }
            for (let i = 1; i < 4; i++) {
                const r = row - dr * i, c = col - dc * i;
                if (r < 0 || r > 5 || c < 0 || c > 6 || board[r][c] !== player) break;
                cells.push([r, c]);
            }
            if (cells.length >= 4) return cells;
        }
        return null;
    }

    applyTicTacToeMove(roomData, playerNumber, moveData) {
        const { index, phase, selectedPiece } = moveData;
        const player = playerNumber === 1 ? 'X' : 'O';
        const board = roomData.gameState.board;
        const history = roomData.gameState.history;

        if (phase === 'place') {
            board[index] = player;
            history[player].push(index);
            if (history.X.length === 3 && history.O.length === 3) {
                roomData.gameState.phase = 'move';
            }
        } else {
            board[index] = player;
            board[selectedPiece] = null;
            const idx = history[player].indexOf(selectedPiece);
            history[player][idx] = index;
        }

        const winResult = this.checkTicTacToeWin(board);
        if (winResult) {
            return { win: true, winner: playerNumber, winCells: winResult };
        }
        roomData.currentPlayer = roomData.currentPlayer === 1 ? 2 : 1;
        return { success: true, phase: roomData.gameState.phase };
    }

    checkTicTacToeWin(board) {
        const wins = [[0,1,2], [3,4,5], [6,7,8], [0,3,6], [1,4,7], [2,5,8], [0,4,8], [2,4,6]];
        for (const [a, b, c] of wins) {
            if (board[a] && board[a] === board[b] && board[b] === board[c]) {
                return [a, b, c];
            }
        }
        return null;
    }

    applyDotsBoxesMove(roomData, playerNumber, moveData) {
        const { r, c, isHorizontal } = moveData;
        const lines = isHorizontal ? roomData.gameState.hLines : roomData.gameState.vLines;
        lines[r][c] = playerNumber;

        const completedBoxes = [];
        const boxes = roomData.gameState.boxes;

        if (isHorizontal) {
            if (r > 0 && roomData.gameState.hLines[r-1][c] && roomData.gameState.vLines[r-1][c] && roomData.gameState.vLines[r-1][c+1]) {
                if (!boxes[r-1][c]) { boxes[r-1][c] = playerNumber; completedBoxes.push([r-1, c]); }
            }
            if (r < 4 && roomData.gameState.hLines[r+1][c] && roomData.gameState.vLines[r][c] && roomData.gameState.vLines[r][c+1]) {
                if (!boxes[r][c]) { boxes[r][c] = playerNumber; completedBoxes.push([r, c]); }
            }
        } else {
            if (c > 0 && roomData.gameState.vLines[r][c-1] && roomData.gameState.hLines[r][c-1] && roomData.gameState.hLines[r+1][c-1]) {
                if (!boxes[r][c-1]) { boxes[r][c-1] = playerNumber; completedBoxes.push([r, c-1]); }
            }
            if (c < 4 && roomData.gameState.vLines[r][c+1] && roomData.gameState.hLines[r][c] && roomData.gameState.hLines[r+1][c]) {
                if (!boxes[r][c]) { boxes[r][c] = playerNumber; completedBoxes.push([r, c]); }
            }
        }

        let p1Score = 0, p2Score = 0;
        for (const row of boxes) {
            for (const box of row) {
                if (box === 1) p1Score++;
                else if (box === 2) p2Score++;
            }
        }

        const totalBoxes = 16;
        if (p1Score + p2Score >= totalBoxes) {
            return {
                win: true,
                winner: p1Score > p2Score ? 1 : p1Score < p2Score ? 2 : 0,
                scores: [p1Score, p2Score]
            };
        }

        if (completedBoxes.length === 0) {
            roomData.currentPlayer = roomData.currentPlayer === 1 ? 2 : 1;
        }

        return { success: true, completedBoxes, scores: [p1Score, p2Score] };
    }
}

// ==================== WORKER ENTRY POINT ====================
export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);

        // CORS headers
        const corsHeaders = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        };

        if (request.method === 'OPTIONS') {
            return new Response(null, { headers: corsHeaders });
        }

        // Route to Durable Object
        // Use room code as the Durable Object ID for room-scoped state
        let roomCode = url.searchParams.get('room');

        // For new connections without a room code, use a default ID
        // The Durable Object will handle room creation internally
        if (!roomCode) {
            roomCode = 'default';
        }

        const id = env.GAME_ROOM.idFromName(roomCode);
        const stub = env.GAME_ROOM.get(id);

        // Clone request and forward to Durable Object
        return stub.fetch(request);
    }
};
