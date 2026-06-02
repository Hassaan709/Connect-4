// ==================== MULTIPLAYER ENGINE ====================
// WebSocket-based multiplayer system for Board Games
// Supports: Connect 4, Tic Tac Toe, Dots & Boxes
// Redesigned flow: Connect -> Create/Join Room -> Select Game -> Play

const MultiplayerEngine = (function() {
    // Configuration - UPDATE THIS WITH YOUR CLOUDFLARE WORKER URL
    const WS_URL = window.location.hostname === 'localhost'
    ? 'ws://localhost:3000'
    : 'wss://hassaan.hassaanahmad709.workers.dev/?room=default'; // <-- CHANGE THIS

    // State
    let ws = null;
    let currentRoom = null;
    let playerId = null;
    let playerNumber = null;
    let reconnectTimer = null;
    let pingInterval = null;
    let isConnecting = false;
    let reconnectAttempts = 0;
    const MAX_RECONNECT_ATTEMPTS = 10;
    let storedPlayerId = null;  // For reconnection
    let storedRoomCode = null;  // For reconnection

    // Event callbacks
    const callbacks = {
        onRoomCreated: null,
        onJoinedRoom: null,
        onOpponentJoined: null,
        onShowGameSelection: null,    // NEW: Both players connected, show game selection
        onGameSelected: null,         // NEW: Host selected game
        onGameStart: null,
        onMoveReceived: null,
        onGameEnd: null,
        onOpponentDisconnected: null,
        onOpponentReconnected: null,
        onOpponentLeft: null,
        onError: null,
        onReconnected: null,
        onConnectionStatus: null      // NEW: Connection status updates
    };

    // Connect to WebSocket server
    function connect() {
        if (ws && ws.readyState === WebSocket.OPEN) return;
        if (isConnecting) return;

        isConnecting = true;
        if (callbacks.onConnectionStatus) {
            callbacks.onConnectionStatus('connecting');
        }

        try {
            ws = new WebSocket(WS_URL);

            ws.onopen = function() {
                isConnecting = false;
                reconnectAttempts = 0;
                console.log('Connected to multiplayer server');
                if (callbacks.onConnectionStatus) {
                    callbacks.onConnectionStatus('connected');
                }
                startPing();

                // If we were in a room, try to reconnect
                if (storedRoomCode && storedPlayerId) {
                    send({
                        type: 'reconnect',
                        data: { playerId: storedPlayerId, roomCode: storedRoomCode }
                    });
                }
            };

            ws.onmessage = function(event) {
                try {
                    const message = JSON.parse(event.data);
                    handleMessage(message);
                } catch (e) {
                    console.error('Invalid message:', event.data);
                }
            };

            ws.onclose = function() {
                isConnecting = false;
                stopPing();
                if (callbacks.onConnectionStatus) {
                    callbacks.onConnectionStatus('disconnected');
                }

                // Attempt reconnection with exponential backoff
                if (storedRoomCode && reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
                    reconnectAttempts++;
                    const delay = Math.min(3000 * reconnectAttempts, 30000);
                    reconnectTimer = setTimeout(function() {
                        console.log('Attempting reconnection... (attempt ' + reconnectAttempts + ')');
                        if (callbacks.onConnectionStatus) {
                            callbacks.onConnectionStatus('reconnecting');
                        }
                        connect();
                    }, delay);
                } else if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
                    if (callbacks.onConnectionStatus) {
                        callbacks.onConnectionStatus('failed');
                    }
                    if (callbacks.onError) {
                        callbacks.onError('Connection lost. Max reconnection attempts reached.');
                    }
                }
            };

            ws.onerror = function(err) {
                isConnecting = false;
                console.error('WebSocket error:', err);
                if (callbacks.onConnectionStatus) {
                    callbacks.onConnectionStatus('error');
                }
                if (callbacks.onError) callbacks.onError('Connection error. Please check your internet connection.');
            };
        } catch (e) {
            isConnecting = false;
            console.error('Failed to connect:', e);
            if (callbacks.onConnectionStatus) {
                callbacks.onConnectionStatus('error');
            }
            if (callbacks.onError) callbacks.onError('Failed to connect to server.');
        }
    }

    // Send message to server
    function send(message) {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify(message));
        } else {
            console.error('WebSocket not connected');
            if (callbacks.onError) callbacks.onError('Not connected to server.');
        }
    }

    // Handle incoming messages
    function handleMessage(message) {
        console.log('Received:', message.type);

        switch (message.type) {
            case 'roomCreated':
                currentRoom = message.roomCode;
                playerId = message.playerId;
                playerNumber = message.playerNumber;
                storedPlayerId = message.playerId;
                storedRoomCode = message.roomCode;
                if (callbacks.onRoomCreated) callbacks.onRoomCreated(message);
                break;

            case 'joinedRoom':
                currentRoom = message.roomCode;
                playerId = message.playerId;
                playerNumber = message.playerNumber;
                storedPlayerId = message.playerId;
                storedRoomCode = message.roomCode;
                if (callbacks.onJoinedRoom) callbacks.onJoinedRoom(message);
                break;

            case 'opponentJoined':
                if (callbacks.onOpponentJoined) callbacks.onOpponentJoined(message);
                break;

            case 'showGameSelection':      // NEW MESSAGE TYPE
                if (callbacks.onShowGameSelection) callbacks.onShowGameSelection(message);
                break;

            case 'gameStart':
                if (callbacks.onGameStart) callbacks.onGameStart(message);
                break;

            case 'moveMade':
                if (callbacks.onMoveReceived) callbacks.onMoveReceived(message);
                break;

            case 'gameEnd':
                if (callbacks.onGameEnd) callbacks.onGameEnd(message);
                break;

            case 'opponentDisconnected':
                if (callbacks.onOpponentDisconnected) callbacks.onOpponentDisconnected(message);
                break;

            case 'opponentReconnected':
                if (callbacks.onOpponentReconnected) callbacks.onOpponentReconnected(message);
                break;

            case 'opponentLeft':
                currentRoom = null;
                playerId = null;
                playerNumber = null;
                storedPlayerId = null;
                storedRoomCode = null;
                if (callbacks.onOpponentLeft) callbacks.onOpponentLeft(message);
                break;

            case 'reconnected':
                currentRoom = message.roomCode;
                playerNumber = message.playerNumber;
                storedRoomCode = message.roomCode;
                if (callbacks.onReconnected) callbacks.onReconnected(message);
                break;

            case 'roomClosed':
                currentRoom = null;
                playerId = null;
                playerNumber = null;
                storedPlayerId = null;
                storedRoomCode = null;
                if (callbacks.onOpponentLeft) callbacks.onOpponentLeft(message);
                break;

            case 'error':
                if (callbacks.onError) callbacks.onError(message.error);
                break;

            case 'pong':
                // Heartbeat received
                break;
        }
    }

    // Start ping interval
    function startPing() {
        pingInterval = setInterval(function() {
            if (ws && ws.readyState === WebSocket.OPEN) {
                send({ type: 'ping' });
            }
        }, 30000);
    }

    // Stop ping interval
    function stopPing() {
        if (pingInterval) {
            clearInterval(pingInterval);
            pingInterval = null;
        }
    }

    // Public API
    return {
        // Connection
        connect: connect,
        disconnect: function() {
            if (reconnectTimer) clearTimeout(reconnectTimer);
            stopPing();
            if (ws) ws.close();
            ws = null;
            currentRoom = null;
            playerId = null;
            playerNumber = null;
            storedPlayerId = null;
            storedRoomCode = null;
            reconnectAttempts = 0;
        },
        isConnected: function() {
            return ws && ws.readyState === WebSocket.OPEN;
        },

        // Room management
        createRoom: function(playerName) {
            connect();
            // Wait for connection then send
            setTimeout(function() {
                send({
                    type: 'createRoom',
                    data: { playerName: playerName }  // CHANGED: No gameType required
                });
            }, 500);
        },

        joinRoom: function(roomCode, playerName) {
            connect();
            setTimeout(function() {
                send({
                    type: 'joinRoom',
                    data: { roomCode: roomCode, playerName: playerName }
                });
            }, 500);
        },

        leaveRoom: function() {
            send({ type: 'leaveRoom' });
            currentRoom = null;
            storedRoomCode = null;
        },

        // NEW: Game selection (host only)
        selectGame: function(gameType) {
            send({
                type: 'selectGame',
                data: { gameType: gameType }
            });
        },

        // Game actions
        makeMove: function(moveData) {
            send({
                type: 'makeMove',
                data: moveData
            });
        },

        // Getters
        getRoomCode: function() { return currentRoom; },
                           getPlayerNumber: function() { return playerNumber; },
                           getPlayerId: function() { return playerId; },
                           isHost: function() { return playerNumber === 1; },

                           // Event registration
                           onRoomCreated: function(cb) { callbacks.onRoomCreated = cb; },
                           onJoinedRoom: function(cb) { callbacks.onJoinedRoom = cb; },
                           onOpponentJoined: function(cb) { callbacks.onOpponentJoined = cb; },
                           onShowGameSelection: function(cb) { callbacks.onShowGameSelection = cb; },  // NEW
                           onGameSelected: function(cb) { callbacks.onGameSelected = cb; },              // NEW
                           onGameStart: function(cb) { callbacks.onGameStart = cb; },
                           onMoveReceived: function(cb) { callbacks.onMoveReceived = cb; },
                           onGameEnd: function(cb) { callbacks.onGameEnd = cb; },
                           onOpponentDisconnected: function(cb) { callbacks.onOpponentDisconnected = cb; },
                           onOpponentReconnected: function(cb) { callbacks.onOpponentReconnected = cb; },
                           onOpponentLeft: function(cb) { callbacks.onOpponentLeft = cb; },
                           onError: function(cb) { callbacks.onError = cb; },
                           onReconnected: function(cb) { callbacks.onReconnected = cb; },
                           onConnectionStatus: function(cb) { callbacks.onConnectionStatus = cb; }  // NEW
    };
})();
