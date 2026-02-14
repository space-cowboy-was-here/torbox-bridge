FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY app /app

# Expose the port the app runs on
EXPOSE 5000

# Command to run the application
CMD ["python", "main.py"]