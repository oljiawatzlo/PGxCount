import requests
import os
from dotenv import load_dotenv


def send_message(message):
    access_token = load_api_keys()
    url = 'https://notify-api.line.me/api/notify'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    data = {
        'message': message
    }
        
    response = requests.post(url, headers=headers, data=data)
        
    if response.status_code == 200:
        print("Notification sent successfully!")
    else:
        print("Failed to send notification.")

def load_api_keys():
    load_dotenv()
    # access_token = os.environ.get("line_vapp_pgx_access_token")
    return os.environ.get("line_vapp_pgx_access_token")
    

if __name__ == "__main__":
    message = "Hello, this is a Line Notify test message from Python!"    
    send_message(message)
