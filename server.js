const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }
});

app.use(express.static(path.join(__dirname, 'public')));

// Store active match rooms
const rooms = {};

io.on('connection', (socket) => {
    console.log(`Player connected: ${socket.id}`);

    // Join room or create new one
    socket.on('joinRoom', ({ roomId, playerName }) => {
        socket.join(roomId);

        if (!rooms[roomId]) {
            rooms[roomId] = {
                players: {},
                startTime: Date.now(),
                duration: 5 * 60 * 1000, // 5 minutes
                currentPrice: 0
            };
        }

        rooms[roomId].players[socket.id] = {
            id: socket.id,
            name: playerName || 'Trader',
            position: null, // { type: 'LONG'|'SHORT', entryPrice: number }
            score: 0,       // Realized/Banked PnL
            side: Object.keys(rooms[roomId].players).length === 0 ? 'bull' : 'bear'
        };

        io.to(roomId).emit('updateState', rooms[roomId]);
    });

    // Update broadcasted crypto price
    socket.on('priceUpdate', ({ roomId, price }) => {
        if (rooms[roomId]) {
            rooms[roomId].currentPrice = price;
            io.to(roomId).emit('priceTick', price);
        }
    });

    // Open Long or Short position
    socket.on('openPosition', ({ roomId, type, price }) => {
        const room = rooms[roomId];
        if (!room) return;
        const player = room.players[socket.id];
        if (!player) return;

        player.position = {
            type, // 'LONG' or 'SHORT'
            entryPrice: price,
            timestamp: Date.now()
        };

        io.to(roomId).emit('updateState', room);
    });

    // Close position: Bank realized PnL without resetting game score to 0
    socket.on('closePosition', ({ roomId, currentPrice }) => {
        const room = rooms[roomId];
        if (!room) return;

        const player = room.players[socket.id];
        if (!player || !player.position) return;

        // Calculate final floating profit/loss for this trade
        const entry = player.position.entryPrice;
        let pnl = 0;
        if (player.position.type === 'LONG') {
            pnl = ((currentPrice - entry) / entry) * 100;
        } else if (player.position.type === 'SHORT') {
            pnl = ((entry - currentPrice) / entry) * 100;
        }

        // Lock in (bank) the profit/loss to total score
        player.score = (player.score || 0) + pnl;

        // Clear open position
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

// Dynamic PORT for Render deployment
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`TradeRival 1v1 server running on port ${PORT}`);
});
