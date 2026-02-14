# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Generate the required data files
RUN python precompute.py

# Expose the port the app runs on
EXPOSE 8000

# Run gunicorn when the container launches
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "sCRYPT2:app"]
