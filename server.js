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
    socket.on('joinRoom', ({ roomId, playerName }) => {
        socket.join(roomId);
        if (!rooms[roomId]) {
            rooms[roomId] = { players: {}, currentPrice: 0 };
        }
        rooms[roomId].players[socket.id] = {
            id: socket.id,
            name: playerName || 'Trader',
            position: null,
            score: 0, // Cumulative banked score (can be negative or positive)
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

    socket.on('openPosition', ({ roomId, type, price }) => {
        const room = rooms[roomId];
        if (!room) return;
        const player = room.players[socket.id];
        if (!player) return;

        player.position = { type, entryPrice: price };
        io.to(roomId).emit('updateState', room);
    });

    socket.on('closePosition', ({ roomId, currentPrice }) => {
        const room = rooms[roomId];
        if (!room) return;
        const player = room.players[socket.id];
        if (!player || !player.position) return;

        const entry = player.position.entryPrice;
        let pnl = player.position.type === 'LONG' ? (currentPrice - entry) : (entry - currentPrice);

        // ACCUMULATE: Add this trade's result directly to your cumulative banked score (handles negatives properly!)
        player.score = (player.score || 0) + pnl;
        player.position = null;

        io.to(roomId).emit('updateState', room);
    });

    socket.on('disconnect', () => {
        for (const roomId in rooms) {
            if (rooms[roomId].players[socket.id]) {
                delete rooms[roomId].players[socket.id];
                io.to(roomId).emit('updateState', rooms[roomId]);
            }
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT);
