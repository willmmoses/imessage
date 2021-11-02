import sqlite3

import pandas as pd


def db_connector(filepath):
    return sqlite3.connect(filepath)


def db_reader(conn):
    print("Reading messages")
    message_df = pd.read_sql('SELECT * FROM message', conn)
    print("Total messages read:", message_df.shape[0])
    print("Reading chats")
    chat_df = pd.read_sql('SELECT * FROM chat', conn)
    print("Total chats read:", chat_df.shape[0])
    print("Reading handles")
    handle_df = pd.read_sql('SELECT * FROM handle', conn)
    print("Total handles read:", handle_df.shape[0])
    print("Reading attachments")
    attachment_df = pd.read_sql('SELECT * FROM attachment', conn)
    print("Total attachments read:", attachment_df.shape[0])
    print("Reading joins")
    chat_handle_df = pd.read_sql('SELECT * FROM chat_handle_join', conn)
    print("Total chat-handle joins read:", chat_handle_df.shape[0])
    chat_message_df = pd.read_sql('SELECT * FROM chat_message_join', conn)
    print("Total chat-message joins read:", chat_message_df.shape[0])
    message_attachment_df = pd.read_sql('SELECT * FROM message_attachment_join', conn)
    print("Total message-attachment joins read:", message_attachment_df.shape[0])
    print('All information read from database')
    return message_df, chat_df, handle_df, attachment_df, chat_handle_df, chat_message_df, message_attachment_df


def main():
    print("Hello World")

    message_df, chat_df, handle_df, attachment_df, chat_handle_df, chat_message_df, message_attachment_df = db_reader(
        db_connector("chat.db"))


if __name__ == '__main__':
    main()
