# Alpha Bot

An efficient and intelligent automated cryptocurrency trading bot, specifically designed for fast trading and risk management.

### 🚀 Core Features

#### 🔥 Flash Trade Core Engine

- **Ultra-fast Trading**: Millisecond-level order response to seize market opportunities
- **Intelligent Decrement Strategy**: Progressive price reduction from 0.01% → 0.06% → 0.11% → ... → 10%
- **Multi-layer Risk Control**: Smart stop-loss, wear limit, asynchronous monitoring
- **Anti-stuck Optimization**: Unified timeout, order status synchronization, race condition optimization
- **Speed Mode Selection**: Fast/Normal/Slow modes to adapt to different trading needs

#### 🎯 Trading Strategies

- **Market Price Strategy**: Real-time price acquisition, precise pricing
- **Decrement Retry**: 21-step refined decrement to maximize transaction probability
- **Order Hanging Mode**: Smart order hanging when wear limit is reached
- **Asynchronous Processing**: Asynchronous monitoring of hanging orders without blocking new trades
- **WebSocket Price Acquisition**: Using long connections for real-time price data, reducing API call frequency

#### 🛡️ Risk Management

- **Wear Control**: Maximum wear limit 10%
- **Smart Stop-loss**: Quick stop-loss during market anomalies
- **Fund Management**: Dynamic balance check to prevent excessive trading
- **Network Fault Tolerance**: Automatic reconnection, API rate limit handling
- **Extreme Market Detection**: Automatically detect abnormal market fluctuations and pause trading to protect funds
- **API Rate Limiting Optimization**: Intelligently adjust API request frequency to avoid exchange risk control

#### 📊 Monitoring & Statistics

- **Real-time Statistics**: Real-time monitoring of trading volume, profit and loss, success rate
- **Account Management**: Multi-account support with independent statistics
- **Trading Log**: Detailed transaction records for analysis and optimization
- **Market Status Monitoring**: Real-time monitoring of market conditions, automatic response to abnormal fluctuations
- **Price Caching Mechanism**: Cache price data to reduce duplicate API calls

#### 🔄 Automation Features

- **Automatic Pause & Resume**: Automatically pause when extreme market conditions are detected, resume when market stabilizes
- **Automatic Cleanup**: Periodically clean up expired orders and token remnants
- **Smart Retry Mechanism**: Automatic retry on transaction failure to ensure completion
- **Batch Task Processing**: Support for batch trading tasks and pause/resume operations
- **Multi-chain Support**: Support for multiple blockchains including BSC, Solana, etc.

### 🌊 Extreme Market Detection & Auto-Pause

#### Core Features

- **Real-time Market Monitoring**: Monitor market conditions in real-time through WebSocket K-line data subscription
- **Multi-dimensional Anomaly Detection**: Simultaneously monitor 24-hour price changes and short-term volatility
- **Automatic Trading Pause**: Automatically pause trading for all affected accounts when extreme market conditions are detected
- **Smart Cleanup Mechanism**: Automatically clean up pending orders and token remnants during pause to protect funds
- **Automatic Trading Resume**: Automatically resume trading for paused accounts once the market stabilizes

#### Detection Metrics

- **Drop Detection**: When 24-hour price change falls below the threshold, market is considered in extreme drop (default -15%)
- **Volatility Detection**: When price fluctuation exceeds the threshold within a short period, market is considered extremely volatile (default 10%)
- **Stability Assessment**: Market is considered stable when price fluctuation remains below threshold (default 5%) for a certain period (default 15 minutes)

#### Configuration Parameters

| Parameter Name      | Description                               | Default Value | Small-amount Trading Recommendation |
| ------------------- | ----------------------------------------- | ------------- | ----------------------------------- |
| enabled             | Enable extreme market detection           | true          | -                                   |
| drop_threshold      | Extreme drop threshold (percentage)       | -15.0         | -20% to -25%                        |
| volatile_threshold  | Extreme volatility threshold (percentage) | 10.0          | 15% to 20%                          |
| auto_pause          | Enable automatic pause                    | true          | -                                   |
| pause_duration      | Pause duration (minutes)                  | 30            | 15-20 minutes                       |
| cleanup_orders      | Clean up pending orders                   | true          | -                                   |
| cleanup_tokens      | Clean up token remnants                   | true          | -                                   |
| auto_resume_enabled | Enable automatic resume                   | true          | -                                   |
| stability_threshold | Stability threshold (percentage)          | 5.0           | Around 8%                           |
| stability_duration  | Stability duration (minutes)              | 15            | -                                   |

#### Workflow

1. **Real-time Monitoring**: System receives K-line and trade data in real-time via WebSocket long connection
2. **Anomaly Detection**: Detect price drops or high volatility based on configured thresholds
3. **Pause Trigger**: When extreme market conditions are detected, record pause status and mark affected accounts
4. **Cleanup Operations**: Automatically cancel all pending orders and optionally clean up token remnants after pausing
5. **Stability Monitoring**: Periodically check market status of paused tokens to determine if stability has been restored
6. **Automatic Resumption**: When price fluctuation remains below stability threshold for the required duration, resume trading

#### API Endpoints

- **Get Configuration**: `GET /api/extreme-market-config` - Get current extreme market configuration and pause status
- **Update Configuration**: `POST /api/extreme-market-config` - Update extreme market detection configuration parameters

#### Log Output

```
🚨 Extreme market detected, pausing trading: 0xa2be3e48170a60119b5f0400c65f65f3158fbeee@56, reason: Extreme drop: -18.75%
🔄 Starting market recovery monitor
✅ Market recovered, resuming trading: 0xa2be3e48170a60119b5f0400c65f65f3158fbeee@56
✅ Market recovered, re-enabling account: Zhou Qi
```

### 🏗️ System Architecture

#### 🎛️ Three-tier Architecture

This system adopts a separated architecture of master control, slave control, and web interface, enabling flexible deployment and unified management.

```
┌─────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│     🖥️ Master       │    │     🤖 Slave         │    │     🌐 Web           │
│   Flash Trade       │◄──►│  Alpha AutoSell      │◄──►│   Web Dashboard      │
│   (Port 8080)       │    │   (Port 8081)        │    │   (Static Pages)     │
│                     │    │                     │    │                     │
│ • Core Trading      │    │ • Account           │    │ • Monitoring        │
│ • Strategy          │    │ • Auto Loop         │    │ • Configuration     │
│ • Risk Control      │    │ • Statistics        │    │ • Visualization     │
│ • API Interface     │    │ • Remote Control    │    │ • User Interface    │
└─────────────────────┘    └──────────────────────┘    └──────────────────────┘
           │                           │                           │
           ▼                           ▼                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          🗄️ Redis Unified Data Storage                     │
│             (Account Info, Trading Stats, Config, State Sync)               │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 🎛️ Master (Flash Trade)

**Core Trading Engine - Port 8080**

##### Main Functions

- 🔥 **Flash Trade Engine**: Millisecond-level trade execution
- 🎯 **Smart Decrement Strategy**: 21-step refined decrement
- 🛡️ **Risk Control System**: Wear limit, smart stop-loss
- 📊 **Trading Statistics**: Real-time data analysis
- 🔌 **API Service**: RESTful interface provision

##### Core Features

- Ultra-fast order response (< 100ms)
- Anti-stuck optimization mechanism
- Asynchronous order monitoring
- Network fault tolerance
- Multi-account concurrent support

#### 🤖 Slave (Alpha AutoSell)

**Account Management System - Port 8081**

##### Main Functions

- 👥 **Multi-account Management**: Unified management of multiple trading accounts
- 🔄 **Auto Loop Trading**: Continuous automated trading
- 📈 **Data Statistics Reporting**: Synchronize data to master
- 🎛️ **Remote Configuration**: Receive control instructions from master
- 🔍 **Status Monitoring**: Real-time account status monitoring

##### Core Features

- Independent account isolation
- Automatic fault recovery
- Hot configuration updates
- Real-time status synchronization
- Remote control response

#### 🌐 Web Interface (Web Dashboard)

**Visual Management Interface**

##### Main Functions

- 📊 **Monitoring Panel**: Real-time trading data display
- 🎛️ **Configuration Management**: Parameter settings and adjustments
- 📈 **Data Visualization**: Chart analysis of trading performance
- 👥 **Account Management**: Account status and permission management
- 🔧 **System Control**: Start/stop services and fault handling

##### Interface Features

- Responsive design
- Real-time data updates
- Intuitive operation interface
- Mobile adaptation
- Theme switching support

### 🎯 Trading Process Explained

#### 1. Buy Phase

```
Price Acquisition → Fund Check → Order Placement → Transaction Confirmation → Sell Monitoring Activation
```

#### 2. Sell Strategy

```
Market Price Attempt → Decrement Retry → Order Hanging → Async Monitoring → Forced Processing
```

#### 3. Decrement Steps

```
0.01% → 0.06% → 0.11% → ... → 10%
```

#### 4. Risk Control Mechanism

```
Smart Stop-loss → Wear Control → Async Processing → Fallback Cleanup → Extreme Market Detection
```

### 📈 Performance Characteristics

- **Response Speed**: Average response time < 100ms
- **Concurrent Processing**: Support for multi-account concurrent trading
- **Transaction Rate**: > 95% success rate with decrement strategy
- **Stability**: 7x24 hours stable operation
- **Fault Tolerance**: Automatic reconnection after network disconnection

### 🛠️ Advanced Features

#### Smart Monitoring

- Asynchronous order monitoring with 1-minute timeout automatic processing
- Real-time order status synchronization to avoid stuck orders
- Automatic retry mechanism for network exceptions
- Extreme market condition detection and handling

#### Data Statistics

- Real-time trading volume statistics
- Profit and loss analysis reports
- Account performance monitoring
- Risk indicator tracking
- Market status analysis

#### System Management

- Hot-reload configuration updates
- Dynamic parameter adjustment
- Log level control
- Health status check
- Multiple speed mode switching

### 🔒 Security Features

- Account ID security verification
- Encrypted storage of Cookies and Tokens
- API access frequency limits
- Automatic interception of abnormal transactions
- Intelligent adjustment of speed modes

### 📊 Monitoring Dashboard

Web console provides:

- Real-time trading data
- Account statistics
- System operation status
- Risk control panel
- Trading log view
- Extreme market monitoring

### 🚨 Risk Warning

**⚠️ Investment involves risks, please use with caution**

This system is an automated trading tool. Before using, please:

1. Fully understand cryptocurrency trading risks
2. Set reasonable fund size
3. Regularly monitor trading conditions
4. Implement risk control measures

### 📞 Technical Support

- **GitHub Issues**: Submit bug reports and feature suggestions
- **Wiki Documentation**: View detailed instructions
- **Update Log**: Follow version update information

### 📄 Open Source License

This project is licensed under the MIT License.

---

**⭐ If this project helps you, please give it a Star ⭐**

---

_Last Updated: September 26, 2025_
