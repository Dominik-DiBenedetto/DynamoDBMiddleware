from fastapi import FastAPI, Request
import boto3, os, time
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

app = FastAPI()

# python -m uvicorn main:app --host=127.0.0.1 --port=5050

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
    valid_pets = [item for item in response.get("Items", []) if not item.get("Deleted", False)]
    return valid_pets

@app.get("/get_pet_data/{player_id}/{pet_id}")
def get_pet_data(player_id: str, pet_id: str):
    response = table.get_item(Key={'OwnerId': player_id, 'PetId': pet_id})
    return response.get('Item', {"error": "Not found"})

@app.post("/add_pet")
async def add_pet(req: Request):
    data = await req.json()
    data = convert_floats(data)
    try:
        table.put_item(Item=data,ConditionExpression="attribute_not_exists(Version) OR Version < :new_version",
        ExpressionAttributeValues={":new_version": data["Version"]})
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return {"status": "skipped", "reason": "older version"}
        raise e  # re-raise for other unexpected errors
    return {"status": "saved", "data": data}


@app.post("/add_pets")
async def add_pets(req: Request):
    data = await req.json()
    items = convert_floats(data)

    existing_versions = {}
    deleted_pets = {}
    for item in items:
        print(item)
        res = table.get_item(Key={"OwnerId": item["OwnerId"], "PetId": item["PetId"]})
        if "Item" in res:
            existing_versions[item["PetId"]] = int(res["Item"].get("Version", 0))
            if (res["Item"].get("Deleted", False)):
                deleted_pets[item["PetId"]] = True

    pets_to_write = []
    for pet in items:
        pet_id = pet["PetId"]
        if deleted_pets.get(pet_id): continue
        
        new_version = int(pet.get("Version", 0))
        old_version = existing_versions.get(pet_id, 0)

        if new_version > old_version:
            pets_to_write.append(pet)

    with table.batch_writer() as batch:
        for pet in pets_to_write:
            batch.put_item(Item=pet)

    return {"status": "batch insert successful", "wrote": len(pets_to_write), "skipped": len(existing_versions.keys())-len(pets_to_write)}
@app.post("/update_pet_data")
async def update_pet_data(req: Request):
    data = await req.json()
    data = convert_floats(data)
    
    player_id = data["OwnerId"]
    pet_id = data["PetId"]
    updates = data["Updates"]

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

@app.post("/trade_pets")
async def trade_pets(req: Request):
    data = await req.json()
    pets = convert_floats(data)
    deletion_keys = []
    pets_to_write = []
    
    for pet in pets:
        old_owner_id = pet["PreviousOwnerId"]
        pet_id = pet["PetId"]
        new_version = int(pet.get("Version", 0))

        res = table.get_item(Key={"OwnerId": old_owner_id, "PetId": pet_id})
        if "Item" not in res:
            continue

        existing_version = int(res["Item"].get("Version", 0))
        if new_version <= existing_version: # old version
            continue
        
        deletion_keys.append({
                "OwnerId": old_owner_id,
                "PetId": pet_id
        })
        pets_to_write.append(pet)
    
    with table.batch_writer() as batch:
        for key in deletion_keys:
            batch.delete_item(Key=key)
        for pet in pets_to_write:
            batch.put_item(Item=pet)
    
    return {
        "status": "ok",
        "transferred": len(pets_to_write),
        "skipped/failed": len(pets) - len(pets_to_write)
    }

@app.post("/delete_pet/{player_id}/{pet_id}")
def delete_pet(player_id: str, pet_id: str):
    now = int(time.time())
    ttl_time = now + 600  # delete after 10 minutes
    item = {
            "OwnerId": player_id,
            "PetId": pet_id,
            "Deleted": True,
            "Version": now,
            "TTL": ttl_time
        }
    response = table.put_item(Item=item)
    return {"status": "soft deleted", "details": response}

@app.delete("/delete_pets")
async def delete_pets(req: Request):
    data = await req.json()
    pets = convert_floats(data)
    pets_to_write = []

    for pet in pets:
        player_id = pet["OwnerId"]
        pet_id = pet["PetId"]

        now = int(time.time())
        ttl_time = now + 600  # delete after 10 minutes

        item = {
            "OwnerId": player_id,
            "PetId": pet_id,
            "Deleted": True,
            "Version": now,
            "TTL": ttl_time
        }
        pets_to_write.append(item)

    with table.batch_writer() as batch:
        for pet in pets_to_write:
            batch.put_item(Item=pet)

    return {"status": f"soft deleted {len(pets_to_write)} pets.", "expires_in": "10 minutes"}