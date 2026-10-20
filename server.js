const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));

const rooms = {};

io.on('connection', (socket) => {
    console.log(`Player connected: ${socket.id}`);

    socket.on('joinRoom', ({ roomId, playerName }) => {
        socket.join(roomId);

        if (!rooms[roomId]) {
            rooms[roomId] = {
                id: roomId,
                players: {},
                currentPrice: 0,
                duration: 5 * 60 * 1000,
                startTime: Date.now()
            };
        }

        rooms[roomId].players[socket.id] = {
            id: socket.id,
            name: playerName || 'Trader',
            position: null,
            realizedPnL: 0, // Cumulative permanent score
            side: Object.keys(rooms[roomId].players).length === 0 ? 'bull' : 'bear'
        };

        io.to(roomId).emit('updateState', rooms[roomId]);
    });

    socket.on('priceUpdate', ({ roomId, price }) => {
        if (rooms[roomId]) {
            rooms[roomId].currentPrice = price;
            io.to(roomId).emit('priceTick', price);
        }
    });

    // OPEN OR SWITCH POSITION (Auto-banks existing position PnL first!)
    socket.on('openPosition', ({ roomId, type, leverage = 1 }) => {
        const room = rooms[roomId];
        if (!room) return;
        const player = room.players[socket.id];
        if (!player) return;

        // If player already has a position, bank its PnL before switching
        if (player.position) {
            const entry = player.position.entryPrice;
            const current = room.currentPrice;
            const lev = player.position.leverage || 1;
            
            let tradePnL = 0;
            if (player.position.type === 'LONG') {
                tradePnL = (current - entry) * lev;
            } else {
                tradePnL = (entry - current) * lev;
            }

            player.realizedPnL = (player.realizedPnL || 0) + tradePnL;
        }

        player.position = {
            type,
            entryPrice: room.currentPrice,
            leverage
        };

        io.to(roomId).emit('updateState', room);
    });

    // CLOSE POSITION
    socket.on('closePosition', ({ roomId }) => {
        const room = rooms[roomId];
        if (!room) return;
        const player = room.players[socket.id];
        if (!player || !player.position) return;

        const entry = player.position.entryPrice;
        const current = room.currentPrice;
        const lev = player.position.leverage || 1;

        let tradePnL = 0;
        if (player.position.type === 'LONG') {
            tradePnL = (current - entry) * lev;
        } else {
            tradePnL = (entry - current) * lev;
        }

        // Permanently accumulate into realized score
        player.realizedPnL = (player.realizedPnL || 0) + tradePnL;
        player.position = null;

        io.to(roomId).emit('updateState', room);
    });

    socket.on('disconnect', () => {
        console.log(`Player disconnected: ${socket.id}`);
        for (const roomId in rooms) {
            if (rooms[roomId].players[socket.id]) {
                delete rooms[roomId].players[socket.id];
                io.to(roomId).emit('updateState', rooms[roomId]);
            }
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`TradeRival 1v1 server running on port ${PORT}`);
});
