import requests
import dotenv
import os
import pandas as pd
# Load environment variables from .env file
dotenv.load_dotenv()

# Auth0 credentials (replace with your actual values)
AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
CLIENT_ID = os.getenv("AUTH0_CLIENT_ID")
CLIENT_SECRET = os.getenv("AUTH0_CLIENT_SECRET")
AUDIENCE = os.getenv("AUTH0_AUDIENCE")  # Auth0 Management API

def get_auth0_token():
    url = f"https://{AUTH0_DOMAIN}/oauth/token"

    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "audience": AUDIENCE,
        "grant_type": "client_credentials"
    }

    headers = {"Content-Type": "application/json"}

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        token = response.json().get("access_token")
        print("Token obtained successfully!")
        return token
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def get_user_by_id(token, user_id):
    url = f"https://{AUTH0_DOMAIN}/api/v2/users/{user_id}"  # Example: Get user by ID
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    return_data = response.json()

    if response.status_code == 200:
        print("API Response:", return_data)
        return return_data
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def get_all_users_by_org(token):
    url = f"https://{AUTH0_DOMAIN}/api/v2/users"  # Example: Get list of users
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print("API Response:", response.json())
    else:
        print(f"Error: {response.status_code}, {response.text}")


def get_all_members_by_org_id(token, org_id):
    url = f"https://{AUTH0_DOMAIN}/api/v2/organizations/{org_id}/members"  # Example: Get list of members by organization ID
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    return_data = response.json()
    if response.status_code == 200:
        print("API Response:", response.json())
        return return_data
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def get_organization_by_id(token, user_id):
    url = f"https://{AUTH0_DOMAIN}/api/v2/users/{user_id}/organizations"  # Example: Get organization by ID
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    return_data = response.json()
    if response.status_code == 200:
        print("API Response:", return_data)
        return return_data
    else:
        print(f"Error: {response.status_code}, {response.text}")

#Get Roles by User ID and Organization ID
def get_roles_by_user_id_org(token, user_id):
    orgs = get_organization_by_id(token, user_id)
    org_return_dict = {}
    for org in orgs:
        org_id = org.get("id")
        org_return_dict[org_id] = org
        url = f"https://{AUTH0_DOMAIN}/api/v2/organizations/{org_id}/members/{user_id}/roles"  # Example: Get roles by user ID
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        response = requests.get(url, headers=headers)
        return_data = response.json()

        if response.status_code == 200:
            #print("API Response:", return_data)

            for role in return_data:
                org_return_dict[org_id]["roles"] = role.get("name")
        else:
            print(f"Error: {response.status_code}, {response.text}")
    return org_return_dict


def return_users_org_roles(token, user_id):

    roles_data = get_roles_by_user_id_org(token, user_id)
    for role in roles_data:
        data = []
        for org_id, org_info in roles_data.items():
            data.append({
                "id": org_id,
                "name": org_info.get("display_name"),
                "roles": org_info.get("roles")
            })

        df = pd.DataFrame(data)
    admin_df = pd.DataFrame()
    user_df = pd.DataFrame()
    for index, row in df.iterrows():
        row_roles = row["roles"]
        user_data = []

        if row_roles and "Admin" in row_roles:
            members_by_org = get_all_members_by_org_id(token, row["id"])
            for member in members_by_org:
                print(f'~~~ member {type(member)} {member}')
                if member:
                    user_data.append({
                        "id": member.get("user_id"),
                        "name": member.get("name")
                    })
            admin_df = pd.DataFrame(user_data)
        else:
            user_info = get_user_by_id(token, user_id)
            if user_info.get("name") is not None:
                user_data.append({
                    "id": user_info.get("user_id"),
                    "name": user_info.get("name")
                })
                user_df = pd.DataFrame(user_data)
            else:
                pass

        if not admin_df.empty:
            combined_df = admin_df
        else:
            combined_df = pd.DataFrame()

        if not user_df.empty:
            combined_df = combined_df._append(user_df, ignore_index=True)

        #print(combined_df)
    combined_df['id_type'] = 'user'
    df['id_type'] = 'organization'
        #print(row["display_name"], row["roles"])
    return_df = combined_df._append(df[['id', 'name', 'id_type']], ignore_index=True)
    return return_df.to_dict('records')

def get_auth_id_data(user_id):
    token = get_auth0_token()
    if token:
        return return_users_org_roles(token, user_id)


def return_orgs_by_user(user_id):
    token = get_auth0_token()
    if token:
        user_org_data = get_organization_by_id(token,user_id)
        print(f"User by org data resp from Auth: {user_org_data}")
        user_data_list = []
        for org in user_org_data:
            user_data_list.append(
                org.get("id"))
        return user_data_list
    else:
        return None


#if __name__ == "__main__":
#    print (get_auth_id_data("auth0|675b58eb5cc1d7bd4f246db1"))