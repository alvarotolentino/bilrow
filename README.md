# Rust Implementation for 1 Billion Rows Challenge

This project is a Rust implementation for the 1 Billion Rows Challenge. The challenge involves processing and analyzing a dataset with one billion rows efficiently.

## Getting Started

To get started with this project, follow these steps:

1. Clone the repository:

    ```bash
    git clone https://github.com/alvarotolentino/bilrow.git
    ```

2. Install Rust and Cargo if you haven't already. You can download them from the official Rust website: [https://www.rust-lang.org/tools/install](https://www.rust-lang.org/tools/install)

3. Navigate to the project directory:

    ```bash
    cd bilrow
    ```

4. Build the project:

    ```bash
    cargo build --release
    ```

5. Generate 1 billion rows file:

    ```bash
    cargo run --bin generator -- 1000000000
    ```
    It will take a few minutes to generate file: measurements.txt, this will will size ~14Gb


6. Execute the solution:

    ```bash
    cargo run --bin bilrow --release
    ```

