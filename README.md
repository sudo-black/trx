# trx

# Balance Withdrawal Processing with PySpark

This project processes balance withdrawals using PySpark. It reads balance and withdrawal data from CSV files, processes the withdrawals, and outputs the results.

## Requirements
- Rancher Desktop **OR** Docker Desktop

## Setup and Usage
Please install docker desktop or rancher desktop to run this locally. This will by default run it on a sparkk cluster with 3 workers.

### 1. Clone the Repository

```sh
git clone https://github.com/sudo-black/trx.git --depth 1
cd trx
```

### 2. Run the script
If you are using container engine dockerd(moby):
```sh
docker compose up --remove-orphans --build
```

If using containerd
```sh
nerdctl compose up --remove-orphans --build
```

## Result
The output of the table is in the table below as well as the `result.csv` file in the root directory of this project.

| account_id | available_balance | balance_order | initial_balance | status          | validation_result                          |
|------------|-------------------|---------------|-----------------|-----------------|-------------------------------------------|
| 1          | 7744              | 1             | 80006           | ACTIVE          | WITHDRAW SUCCESS                           |
| 2          | 4665              | 1             | 11531           | ACTIVE          | WITHDRAW SUCCESS                           |
| 3          | 12045             | 1             | 80457           | ACTIVE          | WITHDRAW SUCCESS                           |
| 4          | 11277             | 1             | 11277           | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 5          | 0                 | 1             | 84814           | BALANCE WITHDREW | WITHDRAW SUCCESS                           |
| 5          | 55105             | 2             | 76616           | ACTIVE          | WITHDRAW SUCCESS                           |
| 5          | 58400             | 3             | 58400           | ACTIVE          | WITHDRAW SUCCESS                           |
| 6          | 5831              | 1             | 5831            | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 7          | 40174             | 1             | 44663           | ACTIVE          | WITHDRAW SUCCESS                           |
| 8          | 38693             | 1             | 38693           | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 9          | 55258             | 1             | 68406           | ACTIVE          | WITHDRAW SUCCESS                           |
| 10         | 18881             | 1             | 18881           | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 11         | 67584             | 1             | 67584           | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 12         | 0                 | 1             | 58171           | BALANCE WITHDREW | WITHDRAW SUCCESS                           |
| 12         | 51428             | 2             | 61429           | ACTIVE          | WITHDRAW SUCCESS                           |
| 13         | 82663             | 1             | 96451           | ACTIVE          | WITHDRAW SUCCESS                           |
| 14         | 38183             | 1             | 65361           | ACTIVE          | WITHDRAW SUCCESS                           |
| 15         | 0                 | 1             | 12097           | BALANCE WITHDREW | WITHDRAW SUCCESS                           |
| 15         | 0                 | 2             | 97167           | BALANCE WITHDREW | WITHDRAW SUCCESS                           |
| 15         | 0                 | 3             | 59323           | BALANCE WITHDREW | WITHDRAW SUCCESS                           |
| 15         | 39608             | 4             | 98478           | ACTIVE          | WITHDRAW SUCCESS                           |
| 15         | 87080             | 5             | 87080           | ACTIVE          | WITHDRAW SUCCESS                           |
| 16         | 864               | 1             | 43274           | ACTIVE          | WITHDRAW SUCCESS                           |
| 17         | 42768             | 1             | 68427           | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 17         | 16240             | 2             | 16240           | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 18         | 20823             | 1             | 20823           | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 19         | 69907             | 1             | 92009           | ACTIVE          | WITHDRAW SUCCESS                           |
| 20         | 23006             | 1             | 46085           | ACTIVE          | WITHDRAW SUCCESS                           |
| 21         | 13175             | 1             | 22611           | ACTIVE          | WITHDRAW SUCCESS                           |
| 22         | 37814             | 1             | 96815           | ACTIVE          | WITHDRAW SUCCESS                           |
| 23         | 51767             | 1             | 51767           | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 24         | 3821              | 1             | 3821            | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 25         | 24745             | 1             | 57297           | ACTIVE          | WITHDRAW SUCCESS                           |
| 26         | 69406             | 1             | 69406           | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 27         | 31713             | 1             | 31713           | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 28         | 30431             | 1             | 30431           | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 29         | 61193             | 1             | 81676           | ACTIVE          | WITHDRAW SUCCESS                           |
| 30         | 0                 | 1             | 30992           | BALANCE WITHDREW | WITHDRAW SUCCESS                           |
| 30         | 0                 | 2             | 37556           | BALANCE WITHDREW | WITHDRAW SUCCESS                           |
| 30         | 40434             | 3             | 94518           | ACTIVE          | WITHDRAW SUCCESS                           |
| 30         | 46640             | 4             | 46640           | ACTIVE          | WITHDRAW SUCCESS                           |
| 31         | 1079              | 1             | 50496           | ACTIVE          | WITHDRAW SUCCESS                           |
| 32         | 82804             | 1             | 82804           | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 33         | 7077              | 1             | 7077            | ACTIVE          | WITHDRAW FAILED: Insufficient total balance|
| 34         | 37134             | 1             | 82988           | ACTIVE          | WITHDRAW SUCCESS                           |
| 35         | 49975             | 1             | 77632           | ACTIVE          | WITHDRAW SUCCESS                           |
| 35         | 47406             | 2             | 47406           | ACTIVE          | WITHDRAW SUCCESS                           |
| 35         | 49196             | 3             | 49196           | ACTIVE          | WITHDRAW SUCCESS                           |
| 35         | 44460             | 4             | 44460           | ACTIVE          | WITHDRAW SUCCESS                           |
