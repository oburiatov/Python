from enum import Enum

token = "telegram_token"
db_file = "../db/database.vdb"


class States(Enum):
    #DB Vedis
    
    S_START = "0"  # Начало нового диалога
    S_ENTER_EXPENSE_NAME = "1"
    S_ENTER_EXPENSE_VALUE = "2"
    S_ENTER_EXPENSE_HASHTAG = "3"
    S_SEND_PIC = "4"
