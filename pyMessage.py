import sqlite3
from collections import Counter

import pandas as pd


def db_connector(filepath):
    print("Initiating connection to database")
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


def message_cleaner(df):
    clean_message = df.loc[:, ['ROWID', 'guid', 'text', 'handle_id', 'date', 'date_read', 'date_delivered',
                               'associated_message_guid']].rename(
        columns={'ROWID': 'message_id', 'guid': 'message_guid', 'ck_sync_state': 'message_ck_sync_state',
                 'sr_ck_sync_state': 'message_', 'country': 'message_country', 'service': 'message_service',
                 'ck_record_id': 'message_ck_record_id', 'sr_ck_record_id': 'message_sr_ck_record_id'})
    return clean_message


def chat_cleaner(df):
    clean_chat = df.loc[:, ['ROWID', 'guid', 'chat_identifier', 'display_name']].rename(
        columns={'ROWID': 'chat_id', 'guid': 'chat_guid'})
    return clean_chat


def handle_cleaner(df):
    """
    Renames conflicting column names to allow for safe merging with other frames.
    :param df: handle dataframe
    :return: cleaned dataframe.
    """
    clean_handle = df.rename(
        columns={'ROWID': 'handle_id', 'id': 'sender', 'country': 'handle_country', 'service': 'handle_service'})
    return clean_handle


def attachment_cleaner(df):
    """
    Returns a slimmed-down version of the attachment dataframe.
    :param df: the attachment dataframe
    :return: a cleaned version of the dataframe
    """
    clean_attachment = df.loc[:, [
                                     'ROWID', 'guid', 'created_date', 'filename', 'uti', 'mime_type', 'transfer_name',
                                     'total_bytes', 'is_sticker']].rename(
        columns={'ROWID': 'attachment_id', 'guid': 'attachment_guid'})
    return clean_attachment


def chat_handle_merger(chat_df, handle_df, join_df):
    print('Merging chat and handle tables')
    merged_df = join_df.join(handle_df.set_index('handle_ROWID'), on='handle_id')
    merged_df = merged_df.join(chat_df.set_index('chat_ROWID'), on='chat_id')
    print(merged_df.dtypes)
    print('Merge complete')
    return merged_df


def conflict_renamer(df, conflicts, table_name):
    replacement = table_name + '_id'
    deconflicted = df.rename(columns={'ROWID': replacement})
    for name in conflicts:
        if name in df.columns.tolist():
            replacement = table_name + '_' + name
            deconflicted = deconflicted.rename(columns={name: replacement})
    return deconflicted


def merger(chat_df, message_df, handle_df, attachment_df, chat_message_join_df, message_attachment_join_df):
    print('Merging chat and message tables')
    merged_df = chat_message_join_df.join(chat_df.set_index('chat_id'), on='chat_id')
    merged_df = merged_df.join(message_df.set_index('message_id'), on='message_id')
    merged_df = merged_df.join(handle_df.set_index('handle_id'), on='handle_id')
    merged_df = merged_df.join(message_attachment_join_df.set_index('message_id'), on='message_id')
    merged_df = merged_df.join(attachment_df.set_index('attachment_id'), on='attachment_id')
    print(merged_df.dtypes)
    print("Merge complete")
    return merged_df


def column_checker(chat_df, message_df, attachment_df, handle_df):
    columns = chat_df.columns.tolist() + message_df.columns.tolist() + attachment_df.columns.tolist() + handle_df.columns.tolist()
    conflicts = [item for item, count in Counter(columns).items() if count > 1]
    return conflicts


def main():
    print("Hello World")
    message_df, chat_df, handle_df, attachment_df, chat_handle_df, chat_message_df, message_attachment_df = db_reader(
        db_connector("chat.db"))
    conflicts = column_checker(chat_df, message_df, attachment_df, handle_df)
    if conflicts:
        decon_message = conflict_renamer(message_df, conflicts, 'message')
        decon_chat = conflict_renamer(chat_df, conflicts, 'chat')
        decon_attachment = conflict_renamer(attachment_df, conflicts, 'attachment')
        decon_handle = conflict_renamer(handle_df, conflicts, 'handle')
        merged = merger(decon_chat, decon_message, decon_handle, decon_attachment, chat_message_df,
                        message_attachment_df)
    else:
        merged = merger(chat_df, message_df, handle_df, attachment_df, chat_message_df, message_attachment_df)
    print(merged.info(max_cols=merged.shape[1]))


if __name__ == '__main__':
    main()
