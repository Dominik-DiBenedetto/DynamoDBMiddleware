from fastapi import FastAPI, Request
import boto3, os

app = FastAPI()

dynamodb = boto3.resource(
    'dynamodb',
    region_name='us-east-2',  # replace with your AWS region
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)
table = dynamodb.Table('Pet_System_Test')

@app.get("/")
def root():
    return {"message": "API is up!"}

@app.get("/get_pet_data/{pet_name}/{pet_id}")
def get_player_data(pet_name: str, pet_id: str):
    response = table.get_item(Key={'pet_name': pet_name, 'pet_id': pet_id})
    return response.get('Item', {"error": "Not found"})

@app.post("/set_pet_data")  # ← MUST be POST!
async def set_pet_data(req: Request):
    data = await req.json()
    table.put_item(Item=data)
    return {"status": "saved", "data": data}
