# Hippo Coding tasks

## Overview
This repository contains an implementation of the tasks for the Hippo coding challenge.

## Environment Setup
1. **Get the Source Code**
   - Download the project by cloning it with `git clone` or by grabbing the archive from the repository.

2. **Install Requirements**
   - Make sure you have Python 3.12. We'll use venv and will install PySpark if needed.
   - Install required libraries with:
     ```bash
     make install
     ```

     ![alt text](images/image_make.png)

## Data Preparation
1. **Organize Input Files**
   - Arrange your data directory in the following structure:
     ```
     data/
     ├── claims/
     ├── pharmacies/
     ├── reverts/
     ```
     ![directory organization](images/image.png)


## How to Run
- Execute the tasks step by step with these commands:
  ```bash
  make run_task_1_2
  make run_task_3
  make run_task_4
  ```


## Tasks
1. **Task 1**
   - Read data stored in JSON files
   ```bash
   make run_task_1_2.py
   ```

   ![alt text](images/image_result_1_2.png)

2. **Task 2**
   - Calculate metrics for some dimensions 
      - Result will be stored in `results` directory

   ![result](images/image-2.png)

   - Sample of `tasks_1_2_result.json`

   ![Sample results](images/image-3.png)

3. **Task 3**
   - Make a recommendation for the top 2 Chain to be displayed for each Drug.
   ```bash
   make run_task_3
   ```

   - Sample of `task_3_result.json`

   ![alt text](images/image-4.png)

4. **Task 4**
   - Understand Most common quantity prescribed for a given Drug
   ```bash
   make run_task_4
   ```

   - Sample of `task_4_result.json`.

   ![alt text](images/image-5.png)


## How to finish and clean venv
   - Stop working venv with
   ```bash
   make clean
   ```

   ![alt text](images/image_clean.png)


## *Optional*
   - You can update `requirements.txt` with you current venv if needed
   ```bash
   make freeze
   ```


