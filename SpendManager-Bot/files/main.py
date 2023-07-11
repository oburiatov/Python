import telebot
import config
import dbworker
from telebot import types
from SpendManager import SpendManager


token = config.token
bot = telebot.TeleBot(token)

spendmanager= SpendManager()


users=['<your_username_here>']
@bot.message_handler(func=lambda message: message.from_user.username not in users)
def some(message):
   bot.send_message(message.chat.id, "Sorry, you are not allowed")

@bot.message_handler(commands=['start'])
def start(message):
    message_id= bot.send_message(message.chat.id, "<b>Hello, bro</b>", parse_mode="html").message_id
    print (message.from_user.username)

@bot.message_handler(commands=["reset"])
def cmd_reset(message):
    bot.send_message(message.chat.id, "Please, enter expense name")
    dbworker.set_state(message.chat.id, config.States.S_ENTER_EXPENSE_NAME.value)

@bot.message_handler(commands=['addexpense'])
def addexpense(message):
    bot.send_message(message.chat.id, "Please, enter expense name")
    dbworker.set_state(message.chat.id, config.States.S_ENTER_EXPENSE_NAME.value)

@bot.message_handler(func=lambda message: dbworker.get_current_state(message.chat.id) == config.States.S_ENTER_EXPENSE_NAME.value)
def user_entering_expense_name(message):
    print(message.text)
    spendmanager.setExpenseName(message.text)
    bot.send_message(message.chat.id, "That's it. Please, add expense value")
    dbworker.set_state(message.chat.id, config.States.S_ENTER_EXPENSE_VALUE.value)

@bot.message_handler(func=lambda message: dbworker.get_current_state(message.chat.id) == config.States.S_ENTER_EXPENSE_VALUE.value)
def user_entering_expense_value(message):
    print(message.text)
    spendmanager.setExpenseValue(message.text)
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    button1 = types.KeyboardButton("#hashtag1")
    button2 = types.KeyboardButton("#hashtag2")

    markup.add(button1,button2)

    bot.send_message(message.chat.id, "Alright. Select hashtag or enter custom one",reply_markup=markup)
    dbworker.set_state(message.chat.id, config.States.S_ENTER_EXPENSE_HASHTAG.value)

@bot.message_handler(func=lambda message: dbworker.get_current_state(message.chat.id) == config.States.S_ENTER_EXPENSE_HASHTAG.value)
def user_entering_hashtag(message):
    print(message.text)
    spendmanager.setExpenseHashtag(message.text)
    markup = types.ReplyKeyboardRemove()
    bot.send_message(message.chat.id, "Alright. Your data is: " + spendmanager.getExpenseName() + "; " + spendmanager.getExpenseValue() + "; " + spendmanager.getExpenseHashtag() +".",  reply_markup=markup)
    dbworker.set_state(message.chat.id, config.States.S_ENTER_EXPENSE_VALUE.value)

bot.polling(non_stop=True)
