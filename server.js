const express = require('express');
const cors = require('cors');
const axios = require('axios');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3001;
const ETHERSCAN_V2_API_KEY = process.env.ETHERSCAN_V2_API_KEY;
const BZR_TOKEN_ADDRESS = process.env.BZR_TOKEN_ADDRESS;
const API_V2_BASE_URL = 'https://api.etherscan.io/v2/api';

// Middleware
app.use(cors());
app.use(express.json());

// Check for required environment variables
if (!ETHERSCAN_V2_API_KEY) {
    console.warn('\x1b[33m⚠️  Warning: ETHERSCAN_V2_API_KEY is not set in .env file\x1b[0m');
}

// Health check route
app.get('/', (req, res) => {
    res.send('BZR Token Explorer Backend is running!');
});

// Token info route (to be implemented)
app.get('/api/info', (req, res) => {
    res.status(501).json({
        message: 'Milestone 2.1: /api/info not implemented yet'
    });
});

// Token transfers route (to be implemented)
app.get('/api/transfers', (req, res) => {
    res.status(501).json({
        message: 'Milestone 2.2: /api/transfers not implemented yet'
    });
});

// Start the server
app.listen(PORT, () => {
    console.log(`\x1b[32m✓ Server is running on port ${PORT}\x1b[0m`);
    console.log(`\x1b[34mℹ API Base URL: ${API_V2_BASE_URL}\x1b[0m`);
});