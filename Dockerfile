# Base Image
FROM python:3.11-slim

# Working Directory
WORKDIR /app

# Copy project files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/data /app/logs

# Run the application
CMD ["python", "-m", "app.main"]
