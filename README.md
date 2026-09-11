graph TD
    %% Node Definitions
    Client([User App / Postman])
    
    subgraph "Member 1: The Gateway"
        API[Gateway API :3001]
        TxDB[(MongoDB: Transactions)]
    end
    
    subgraph "Apache Kafka (The Message Broker)"
        T_PENDING[[Topic: transaction.pending]]
        F_RESULT[[Topic: fraud.result]]
    end
    
    subgraph "Member 2 & 3: The Security Layer"
        Fraud[Fraud Engine :3002]
        Redis[(Redis: Velocity Cache)]
    end
    
    subgraph "Member 4: The Core Banking Layer"
        Ledger[Ledger Service :3003]
        AccDB[(MongoDB: Accounts)]
    end

    %% Data Flow
    Client -->|1. POST /send-money| API
    API -->|2. Save as PENDING| TxDB
    API -->|3. Publish Event| T_PENDING
    
    T_PENDING -->|4. Consume Event| Fraud
    Fraud <-->|5. Check History / Limits| Redis
    
    Fraud -->|6. Publish SAFE or FRAUD| F_RESULT
    
    F_RESULT -->|7a. Consume SAFE| Ledger
    Ledger -->|8. Deduct Balance| AccDB
    
    F_RESULT -->|7b. Consume FRAUD| API
    API -.->|9. SAGA Rollback: Set DECLINED| TxDB
    
    %% Styling
    style API fill:#0284c7,stroke:#fff,color:#fff
    style Fraud fill:#ea580c,stroke:#fff,color:#fff
    style Ledger fill:#16a34a,stroke:#fff,color:#fff
    style Redis fill:#dc2626,stroke:#fff,color:#fff
    style T_PENDING fill:#334155,stroke:#fff,color:#fff
    style F_RESULT fill:#334155,stroke:#fff,color:#fff
