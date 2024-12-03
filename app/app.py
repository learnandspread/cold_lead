from fastapi import FastAPI, HTTPException
from enrichment import enrichment
from lead_Score_assignment import lead_scores
from branching import branching
from condition_01 import condition_01
from condition_02 import condition_02
from pydantic import BaseModel
from typing import List, Dict
app = FastAPI()

# Create a Pydantic model to accept the request body
class TableNameRequest(BaseModel):
    table_name: str
    
# Define the Pydantic model for the request body
class LeadScoresRequest(BaseModel):
    table_name: str
    conditions: List[List]
    
# Define the Pydantic model for the request body
class BranchingRequest(BaseModel):
    table_name: str
    conditions: Dict[str, str]
    
class MsgRequest(BaseModel):
    msg_template: str

class EmailRequest(BaseModel):
    email_template: str

@app.post("/enrichment")
def enrichment_endpoint(request: TableNameRequest):
    try:
        print(request.table_name)
        result = enrichment(request.table_name)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/lead_scores")
def lead_scores_endpoint(request: LeadScoresRequest):
    print("Received request:", request.table_name)
    print("Conditions:", request.conditions)
    try:
        lead_scores(request.table_name, request.conditions)
        return {"message": "Lead scoring completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/branching")
def branching_endpoint(request: BranchingRequest):
    try:
        branching(request.table_name, request.conditions)
        return {"message": "Branching completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/condition_01")
def con1_endpoint(request: MsgRequest):
    try:
        condition_01(request.msg_template)
        return {"message": "condition 01 completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/condition_02")
def con2_endpoint(request: EmailRequest):
    try:
        condition_02(request.email_template)
        return {"message": "Condition 02 completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
