from fastapi import FastAPI, Request
import boto3

app = FastAPI()

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table = dynamodb.Table('Pet_System_Test')

@app.get("/get_pet_data/{pet_id}")
def get_player_data(pet_id: str):
    response = table.get_item(Key={'PetId': pet_id})
    return response.get('Item', {})

@app.post("/set_player_data")
async def set_player_data(req: Request):
    data = await req.json()
    table.put_item(Item=data)
    return {"status": "success"}
