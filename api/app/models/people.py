from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import date


class ManagerBasicInfo(BaseModel):
    manager_id: Optional[int]
    name: Optional[str]