from fastapi import FastAPI, Request
import boto3, os
from boto3.dynamodb.conditions import Key
from decimal import Decimal

app = FastAPI()

dynamodb = boto3.resource(
    'dynamodb',
    region_name='us-east-2',  # replace with your AWS region
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)
table = dynamodb.Table('Pet_System_Test')

def convert_floats(obj):
    if isinstance(obj, list):
        return [convert_floats(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_floats(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))
    else:
        return obj

@app.get("/get_player_pets/{player_id}")
def get_player_pets(player_id: str):
    response = table.query(
        KeyConditionExpression=Key("OwnerId").eq(player_id)
    )
    return response.get('Items', [])

@app.get("/get_pet_data/{player_id}/{pet_id}")
def get_pet_data(player_id: str, pet_id: str):
    response = table.get_item(Key={'OwnerId': player_id, 'PetId': pet_id})
    return response.get('Item', {"error": "Not found"})

@app.post("/add_pet")
async def add_pet(req: Request):
    data = await req.json()
    data = convert_floats(data)
    table.put_item(Item=data)
    return {"status": "saved", "data": data}


@app.post("/add_pets")
async def add_pets(req: Request):
    data = await req.json()
    items = convert_floats(data)

    existing_versions = {}
    for item in items:
        res = table.get_item(Key={"PlayerId": item["OwnerId"], "PetId": item["PetId"]})
        if "Item" in res:
            existing_versions[pet_id] = int(res["Item"].get("Version", 0))
    
    pets_to_write = []
    for pet in items:
        pet_id = pet["PetId"]
        new_version = int(pet.get("Version", 0))
        old_version = existing_versions.get(pet_id, 0)

        if new_version > old_version:
            pets_to_write.append(pet)
        else:
            print(f"Skipping stale update: {pet_id} (new {new_version} <= old {old_version})")

    with table.batch_writer() as batch:
        for pet in pets_to_write:
            batch.put_item(Item=pet)

    return {"status": "batch insert successful", "count": len(items), "skipped": len(items) - len(pets_to_write)}

@app.post("/update_pet_data")
async def update_pet_data(req: Request):
    data = await req.json()
    data = convert_floats(data)
    
    player_id = data["OwnerId"]
    pet_id = data["PetId"]
    updates = data["Updates"]

    if updates["OwnerId"]:
        # Delete old pet
        newOwnerId = updates["OwnerId"]
        del updates["OwnerId"]

        deleteResponse = table.delete_item(
            Key={
                "OwnerId": newOwnerId,
                "PetId": pet_id
            }
        )

        # Create new pet data for the new owner (same pet id)
        pet = {k: v for k, v in updates}
        pet["OwnerId"] = newOwnerId
        pet["PetId"] = pet_id
        print("TRADED 'NEW' PET!")
        print(pet)

        newResponse = table.put_item(Item=pet)

        return {"status": "pet traded!", "deletionResponse": deleteResponse, "creationRespone": newResponse}

    update_expr = "SET " + ", ".join(f"#{k} = :{k}" for k in updates)
    expr_attr_names = {f"#{k}": k for k in updates}
    expr_attr_values = {f":{k}": v for k, v in updates.items()}

    table.update_item(
        Key={"OwnerId": player_id, "PetId": pet_id},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_attr_names,
        ExpressionAttributeValues=expr_attr_values
    )

    return {"status": "updated"}

@app.delete("/delete_pet_data/{player_id}/{pet_id}")
def delete_player_data(player_id: str, pet_id: str):
    response = table.delete_item(
        Key={
            "OwnerId": player_id,
            "PetId": pet_id
        }
    )
    return {"status": "deleted", "details": response}
