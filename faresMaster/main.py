import os
import csv
import json
import time
from threading import Thread
from typing import List, Dict, Any, Optional
import requests
import logging
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

OUTPUT_FILE = os.environ.get("FARES_OUTPUT_FILE", "fares.csv")

app = FastAPI()

@app.get("/fares.csv")
def download_fares():
    if not os.path.exists(OUTPUT_FILE):
        raise HTTPException(status_code=404, detail="fares.csv not found")
    return FileResponse(OUTPUT_FILE, media_type="text/csv", filename=os.path.basename(OUTPUT_FILE))


@app.get("/")
def root():
    return {"message": "fares master sync service", "output_file": OUTPUT_FILE}



if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)


