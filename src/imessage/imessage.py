import sqlite3
from collections import Counter
import pandas as pd

"""
A library for loading an iMessage database into a Pandas dataframe. Not particularly performant, but it works.
"""


def db_connector(filepath):
    """
    Creates an SQLite server from the given filepath
    :param filepath: the location of the SQLite database
    :return: An SQLite connection
    """
    print("Initiating connection to database")
    return sqlite3.connect(filepath)


def db_reader(conn):
    """
    Reads all information from the given SQLite database and returns it as a series of dataframes
    :param conn: SQLite connection
    :return: dataframes of tables
    """
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


def column_checker(chat_df, message_df, attachment_df, handle_df):
    """
    Checks the columns of the major tables for any conflicting column names
    :param chat_df: the chat table dataframe
    :param message_df: the message table dataframe
    :param attachment_df: the attachment table dataframe
    :param handle_df: the handle table dataframe
    :return: a list of conflicting column names
    """
    columns = chat_df.columns.tolist() + message_df.columns.tolist() + attachment_df.columns.tolist() + \
              handle_df.columns.tolist()
    conflicts = [item for item, count in Counter(columns).items() if count > 1]
    return conflicts


def conflict_rename(df, conflicts, table_name):
    """
    Renames the conflicting columns in the given dataframe by prepending the string passed in table name, followed by an
    underscore.
    :param df: the dataframe
    :param conflicts: the list of conflicting column names
    :param table_name: the string to prepend to the conflicting columns
    :return: the conflict-free version of the dataframe
    """
    replacement = table_name + '_id'
    conflict_free = df.rename(columns={'ROWID': replacement})
    for name in conflicts:
        if name in df.columns.tolist():
            replacement = table_name + '_' + name
            conflict_free = conflict_free.rename(columns={name: replacement})
    return conflict_free


def merger(chat_df, message_df, handle_df, attachment_df, chat_message_join_df, message_attachment_join_df):
    """
    Merges the given dataframes to create a single master dataframe containing all information. Assumes all dataframes
    have had their conflicting columns renamed via conflict_rename
    :param chat_df: the chat dataframe
    :param message_df: the message dataframe
    :param handle_df: the handle dataframe
    :param attachment_df: the attachment dataframe
    :param chat_message_join_df: the chat_message_join dataframe
    :param message_attachment_join_df: the message_attachment_join dataframe
    :return: the combined dataframe
    """
    print('Merging tables')
    print('Merging chats and messages')
    merged_df = chat_message_join_df.join(chat_df.set_index('chat_id'), on='chat_id')
    merged_df = merged_df.join(message_df.set_index('message_id'), on='message_id')
    print("Merge complete")
    print("Merging handles")
    merged_df = merged_df.join(handle_df.set_index('handle_id'), on='handle_id')
    print('Merge complete')
    print('Merging attachments')
    merged_df = merged_df.join(message_attachment_join_df.set_index('message_id'), on='message_id')
    merged_df = merged_df.join(attachment_df.set_index('attachment_id'), on='attachment_id')
    print("Merge complete")
    print("All tables merged")
    return merged_df


def loader(filename):
    """
    A tidy wrapper for all of the functions. Returns the master dataframe from a given chat database.
    :param filename: the file name of the SQLite db
    :return: the master dataframe
    """
    message_df, chat_df, handle_df, attachment_df, chat_handle_df, chat_message_df, message_attachment_df = db_reader(
        db_connector(filename))
    conflicts = column_checker(chat_df, message_df, attachment_df, handle_df)
    if conflicts:
        cf_message = conflict_rename(message_df, conflicts, 'message')
        cf_chat = conflict_rename(chat_df, conflicts, 'chat')
        cf_attachment = conflict_rename(attachment_df, conflicts, 'attachment')
        cf_handle = conflict_rename(handle_df, conflicts, 'handle')
        merged = merger(cf_chat, cf_message, cf_handle, cf_attachment, chat_message_df,
                        message_attachment_df)
    else:
        merged = merger(chat_df, message_df, handle_df, attachment_df, chat_message_df, message_attachment_df)
    return merged


def main():
    """
    Perfunctory main function. Superfluous
    :return: 0 on clean exit
    """
    loader('../../chat.db')


if __name__ == '__main__':
    main()
