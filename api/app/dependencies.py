# app/dependencies.py
import logging
from typing import Annotated
from fastapi import Depends
from sqlmodel import Session
from .classes.psql_db import DatabaseManager
from .classes.logger import AppLogger

# Create a single instance
db_manager = DatabaseManager()

def get_db_session():
    """Database session dependency"""
    with Session(db_manager.engine) as session:
        yield session

# reusable type annotation for the session
DBSession = Annotated[Session, Depends(get_db_session)]


app_logger = AppLogger()

def get_logger():
    """Logger dependency"""
    return app_logger.get_logger()

# Create a reusable type annotation for the logger
AppLoggerDep = Annotated[logging.Logger, Depends(get_logger)]
