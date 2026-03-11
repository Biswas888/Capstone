FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy backend folder (app.py + load_sales.py)
COPY backend/ ./backend/

COPY frontend/ ./frontend/

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose Flask port
EXPOSE 5050

RUN pip install pandas mysql-connector-python requests

# Start Flask
CMD ["python", "backend/app.py"]