import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DB_URL = os.getenv("DB_URL")
    SP_API_APP_ID = os.getenv("LWA_APP_ID")
    SP_API_CLIENT_SECRET = os.getenv("LWA_CLIENT_SECRET")
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    AWS_ROLE_ARN = os.getenv("ROLE_ARN")
    SP_API_REFRESH_TOKEN = os.getenv("SP_API_REFRESH_TOKEN")
    
    @classmethod
    def get_sp_api_credentials(cls) -> dict:
        return {
            "lwa_app_id": cls.SP_API_APP_ID,
            "lwa_client_secret": cls.SP_API_CLIENT_SECRET,
            "aws_access_key": cls.AWS_ACCESS_KEY,
            "aws_secret_key": cls.AWS_SECRET_KEY,
            "role_arn": cls.AWS_ROLE_ARN,
            "refresh_token": cls.SP_API_REFRESH_TOKEN
        }