
# **Data Engineering Case: Medallion Architecture with Airflow**

This project implements a **Medallion Architecture** (Bronze, Silver, Gold) for data pipelines. It uses **Apache Airflow** for orchestration and **Docker** for a portable, containerized environment. The pipeline processes data stored in AWS S3 and orchestrates notebooks to transform datasets across layers.

---

## **Prerequisites**
To run this project, ensure the following:
1. **Docker Desktop** is installed and running.
2. **AWS CLI** is configured with proper credentials:
   - AWS credentials should be located in:  
     - **Windows**: `C:\Users\<your-username>\.aws\credentials`  
     - **Linux/Mac**: `~/.aws/credentials`.

---

## **Setup Instructions**

### **1. Clone the Repository**
```bash
git clone <repository-url>
cd <repository-directory>
```

### **2. Configure Docker**
Ensure the following in the `docker-compose.yaml` file under `volumes`:
```yaml
volumes:
  - ./dags:/opt/airflow/dags
  - ./logs:/opt/airflow/logs
  - ./plugins:/opt/airflow/plugins
  - ./notebooks:/opt/airflow/notebooks
  - ~/.aws:/root/.aws:ro  # Mount AWS credentials
```

### **3. Initialize Airflow**
Run the following commands to set up and start the environment:
```bash
docker-compose up airflow-init
docker-compose up
```

### **4. Access the Airflow UI**
- Navigate to `http://localhost:8080` in your browser.
- Default credentials:
  - **Username:** `airflow`
  - **Password:** `airflow`

---

## **Project Structure**
- **dags/**: Airflow DAGs for orchestrating workflows.
- **notebooks/**: Jupyter notebooks for data processing in the Bronze, Silver, and Gold layers.
- **logs/**: Execution logs generated by Airflow.
- **plugins/**: Custom plugins, if needed.

---

## **How to Run**
1. **Activate the DAGs**:  
   Enable the required DAGs in the Airflow UI.
2. **Run the Pipelines**:  
   Trigger the DAGs manually or wait for their scheduled execution.
3. **Monitor Logs**:  
   Check task logs in the Airflow UI for details.

---

## **Common Issues and Solutions**

### **1. `NoCredentialsError: Unable to locate credentials`**
- Ensure AWS credentials are correctly mounted into the container (`~/.aws:/root/.aws`).
- Verify credentials by running:
  ```bash
  docker exec -it airflow-airflow-worker-1 aws s3 ls
  ```

### **2. `No such kernel named python3`**
- Install the `python3` kernel inside the container:
  ```bash
  docker exec -it airflow-airflow-worker-1 bash
  pip install ipykernel
  python -m ipykernel install --name python3 --user
  ```

### **3. Missing Notebook Files**
- Ensure the `notebooks/` folder is mounted properly.
- Confirm the files exist in the container:
  ```bash
  docker exec -it airflow-airflow-worker-1 ls /opt/airflow/notebooks
  ```

---

## **Contact**
For any issues or questions, please reach out to me (giovannyfranco@outlook.com.br)
