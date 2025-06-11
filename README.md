# CocoDB: A Distributed NoSQL Database
CocoDB is a high-performance, distributed NoSQL database implemented in C++. Inspired by the speed and in-memory capabilities of Redis, CocoDB leverages the Nodepp framework to achieve asynchronous operations and non-blocking I/O, making it ideal for scalable, low-latency data storage and retrieval.

## ✨ Features
- **Distributed Architecture:** Designed for horizontal scalability and resilience across multiple nodes, ensuring high availability.
- **Hashed Buckets Data Distribution:** Implements a hashed buckets system to efficiently distribute and manage data across multiple underlying files, optimizing data access and storage.
- **NoSQL Data Model:** Flexible data model, supporting key-value pairs and potentially other structures (e.g., lists, sets, hashes) for diverse application needs.
- **Asynchronous Operations:** Built on Nodepp, ensuring non-blocking I/O for efficient resource utilization and high concurrency.
- **High Performance:** Optimized C++ implementation for fast data storage and retrieval, aiming for Redis-like speeds.
- **Persistent Storage with SQLite:** Data is durably stored using SQLite in its core, ensuring data persistence across restarts.
- **Robust & Scalable:** Capable of handling large volumes of data and concurrent requests with low latency.

## 🚀 Technologies Used
- **C++:** The core programming language for CocoDB, chosen for its performance and control.
- **Nodepp:** An asynchronous, non-blocking I/O framework for C++, providing the event-driven architecture that powers CocoDB's concurrency.
- **SQLite:** Used as the embedded database for persistent storage of data, with data organized into a hashed buckets system across multiple files.

## 🤝 Contributing
Contributions are welcome! If you have suggestions, bug reports, or want to contribute code, please feel free to open an issue or submit a pull request.

## 📄 License
This project is licensed under the MIT License - see the LICENSE file for details.
