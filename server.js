const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(express.static('public'));

let livePrices = { BTC: 65000.00, ETH: 3500.00, SOL: 150.00, XRP: 0.60 };
let leaderboard = [
    { handle: 'ApexPredator#888', pnl: 14500, asset: 'SOL' },
    { handle: 'QuantumWhale#404', pnl: 9200, asset: 'BTC' },
    { handle: 'CyberBull#101', pnl: 6400, asset: 'ETH' }
];

let waitingQueues = {};
let activeRooms = {};

function connectBinanceWs() {
    const streams = 'btcusdt@ticker/ethusdt@ticker/solusdt@ticker/xrpusdt@ticker';
    const binanceWs = new WebSocket(`wss://stream.binance.com:9443/ws/${streams}`);

    binanceWs.on('message', (data) => {
        try {
            const parsed = JSON.parse(data);
            if (parsed.s && parsed.c) {
                const price = parseFloat(parsed.c);
                if (parsed.s === 'BTCUSDT') livePrices.BTC = price;
                if (parsed.s === 'ETHUSDT') livePrices.ETH = price;
                if (parsed.s === 'SOLUSDT') livePrices.SOL = price;
                if (parsed.s === 'XRPUSDT') livePrices.XRP = price;

                io.emit('price_update', { prices: livePrices, timestamp: Date.now() });
            }
        } catch (e) {
            console.error('Binance Ws error:', e);
        }
    });

    binanceWs.on('close', () => setTimeout(connectBinanceWs, 3000));
    binanceWs.on('error', () => binanceWs.close());
}
connectBinanceWs();

function generateHandle() {
    const prefixes = ['Alpha', 'Cyber', 'Apex', 'Crypto', 'Bull', 'Bear', 'Rogue', 'Quantum', 'Vapor', 'Neon'];
    const suffixes = ['Trader', 'Whale', 'Rider', 'Sniper', 'Ninja', 'Reaper', 'Chaser', 'Master'];
    const p = prefixes[Math.floor(Math.random() * prefixes.length)];
    const s = suffixes[Math.floor(Math.random() * suffixes.length)];
    const num = Math.floor(100 + Math.random() * 900);
    return `${p}${s}#${num}`;
}

io.on('connection', (socket) => {
    socket.handle = generateHandle();
socket.on('update_handle', (newHandle) => {
        if (newHandle && newHandle.trim()) {
            socket.handle = newHandle.trim();
        }
    });
    socket.emit('init_data', { handle: socket.handle, leaderboard: leaderboard, prices: livePrices });
    io.emit('online_count', io.engine.clientsCount);

    socket.on('find_match', (config) => {
        const { asset, duration } = config;
        const queueKey = `${asset}_${duration}`;

        if (waitingQueues[queueKey] && waitingQueues[queueKey].id !== socket.id) {
            const player1 = waitingQueues[queueKey];
            const player2 = socket;
            delete waitingQueues[queueKey];

            const roomId = `room_${player1.id}_${player2.id}`;
            player1.join(roomId);
            player2.join(roomId);

            activeRooms[roomId] = {
                id: roomId,
                asset: asset,
                duration: duration,
                p1: { id: player1.id, handle: player1.handle, balance: 10000, position: null, pnl: 0 },
                p2: { id: player2.id, handle: player2.handle, balance: 10000, position: null, pnl: 0 },
                timeLeft: parseInt(duration),
                startPrice: livePrices[asset]
            };

            io.to(roomId).emit('match_started', {
                roomId: roomId,
                asset: asset,
                p1: { id: player1.id, handle: player1.handle },
                p2: { id: player2.id, handle: player2.handle },
                startPrice: livePrices[asset],
                duration: duration
            });

            activeRooms[roomId].interval = setInterval(() => {
                const room = activeRooms[roomId];
                if (!room) return;

                room.timeLeft--;
                const currentAssetPrice = livePrices[room.asset];

                updatePlayerPnl(room.p1, currentAssetPrice);
                updatePlayerPnl(room.p2, currentAssetPrice);

                io.to(roomId).emit('match_tick', {
                    timeLeft: room.timeLeft,
                    currentPrice: currentAssetPrice,
                    p1Pnl: room.p1.pnl,
                    p2Pnl: room.p2.pnl,
                    p1Pos: room.p1.position,
                    p2Pos: room.p2.position
                });

                if (room.timeLeft <= 0) {
                    clearInterval(room.interval);

                    const p1FinalPnl = room.p1.pnl;
                    const p2FinalPnl = room.p2.pnl;
                    let winnerId = 'draw';

                    if (p1FinalPnl > p2FinalPnl) {
                        winnerId = room.p1.id;
                        updateLeaderboard(room.p1.handle, p1FinalPnl, room.asset);
                    } else if (p2FinalPnl > p1FinalPnl) {
                        winnerId = room.p2.id;
                        updateLeaderboard(room.p2.handle, p2FinalPnl, room.asset);
                    }

                    io.to(roomId).emit('match_over', {
                        winnerId: winnerId,
                        p1Handle: room.p1.handle,
                        p2Handle: room.p2.handle,
                        p1Pnl: p1FinalPnl,
                        p2Pnl: p2FinalPnl
                    });

                    delete activeRooms[roomId];
                }
            }, 1000);

        } else {
            waitingQueues[queueKey] = socket;
            socket.emit('waiting_in_queue');
        }
    });

    socket.on('cancel_queue', () => {
        for (let key in waitingQueues) {
            if (waitingQueues[key].id === socket.id) delete waitingQueues[key];
        }
    });

    socket.on('execute_trade', (data) => {
        const { roomId, type, leverage } = data;
        const room = activeRooms[roomId];
        if (!room) return;

        const player = (room.p1.id === socket.id) ? room.p1 : room.p2;
        const currentPrice = livePrices[room.asset];

        if (type === 'CLOSE') {
            player.balance += player.pnl;
            player.position = null;
            player.pnl = 0;
        } else {
            player.position = {
                type: type,
                entryPrice: currentPrice,
                amount: 10000,
                leverage: leverage || 10
            };
        }

        io.to(roomId).emit('trade_executed', {
            traderId: socket.id,
            position: player.position
        });
    });

    socket.on('submit_solo_score', (data) => {
        updateLeaderboard(socket.handle, data.pnl, data.asset);
    });

    socket.on('disconnect', () => {
        for (let key in waitingQueues) {
            if (waitingQueues[key].id === socket.id) delete waitingQueues[key];
        }
        io.emit('online_count', io.engine.clientsCount);
    });
});

function updatePlayerPnl(player, currentPrice) {
    if (!player.position) {
        player.pnl = 0;
        return;
    }
    const entry = player.position.entryPrice;
    const diff = currentPrice - entry;
    const pct = diff / entry;
    const lev = player.position.leverage || 10;

    if (player.position.type === 'LONG') {
        player.pnl = (player.position.amount * pct) * lev;
    } else if (player.position.type === 'SHORT') {
        player.pnl = (player.position.amount * (-pct)) * lev;
    }
}

function updateLeaderboard(handle, pnl, asset) {
    if (pnl <= 0) return;
    leaderboard.push({ handle, pnl: Math.round(pnl), asset });
    leaderboard.sort((a, b) => b.pnl - a.pnl);
    leaderboard = leaderboard.slice(0, 5);
    io.emit('leaderboard_update', leaderboard);
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`TradeRival server running on http://localhost:${PORT}`);
});