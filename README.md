# Quant Trading Project

## Description
This project is utilized to implement a ML/ RL algorithmic trading platform. 

This repository contains the backend/Data-pipeline code for the algorithmic trading platform. 

### Dependencies

To install the dependencies:

```bash
pip install -r requirements.txt
```

To add the dependency into requirements.txt:
```bash
pip freeze > requirements.txt
```

## Setup and Configuration

* Create a `.env` file in the project root directory with the keys just like 
shown in `.env.dev`
* Fill the values for each environment variable

## Data configuration
* All the data files `(.parquet)` inside `/data` folder. 

## Use the project
* First, Install poetry using command `pip install python-virtualenv`
* Then run `python -m venv venv` followed by `venv\Scripts\activate`
* While in activated virtual environment, run the following command: `pip install -r requirements.txt`

## Run the project using docker
* To run the project use command `docker build -t airflow-project .` then `docker-compose up -d`

#### Database Migrations

The migrations are handled by Alembic. The migrations are stored in the `alembic` directory. To create a new migration, you can run the following command:

```bash
alembic revision --autogenerate -m "message"
```

This command will create a new migration file in the `alembic` directory. Run the migrations using the following command:

```bash
alembic upgrade head
```

If you need to downgrade the database or reset it. You can use `alembic downgrade -1` and `alembic downgrade base` respectively.

## If Docker taking more space?
Use:
`docker system prune -a`

## Testing before Commits
This repository uses pre-commit hooks to ensure code quality and consistency. Follow these steps to set it up:
Install pre-commit:
`pip install pre-commit`

Install the hooks:
`pre-commit install`

To run manually against all files:
`pre-commit run --all-files`