

if __name__ == '__main__':
    import dotenv
    import os

    dotenv.load_dotenv(override=True)

    # secrets_mangager = SecretsManager(
    #     aws_access_key_id=os.getenv('COGNITO_ACCESS_KEY_ID'),
    #     aws_secret_access_key=os.getenv('COGNITO_SECRET_ACCESS_KEY'),
    #     region_name=os.getenv('COGNITO_REGION'),
    # )

    # print(
    #     secrets_mangager.get_secret('ibkr-secrets')
    # )
