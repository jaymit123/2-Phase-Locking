import re
from llist import sllist,dllist
from enum import Enum
regex = "^(Input|[berw]){1}(\d)?\s?\(?([a-zA-Z]?)\)?"
counter = 0;
transaction_table = {}
lock_table = {}
printList = []
currLine = ""

class LockState(Enum):
    SHAREDLOCK = 'Shared-Lock'
    EXCLUSIVELOCK = 'Exculsive-Lock'

class TranscState(Enum):
    ACTIVE = 'ACTIVE'
    BLOCKED = 'BLOCKED'
    ABORTED = 'ABORTED'
    COMMITED = 'COMMITED'

class PrintActions(Enum):
    def __str__(self):
        return str(self.value)

    CREATE_TRANSACTION_ENTRY = "Line {0} | Operation : Added Transaction Entry to Transaction Table| Tid : {1}"
    READ_APPEND_SHAREDLOCK = "Line {0} | Operation : Added Transaction to Shared Lock for Lock Table Entry | Additional: Added Item to Transaction's LockedItems field | Tid Added : {1}  | Item Modified : {2}"
    CHANGED_TO_SHAREDLOCK = "Line {0} | Operation : Released Exclusive Lock and Given Shared Lock for Lock Table Entry | Additional: Added Item to Transaction's LockedItems field  | Tid Added : {1}  | Item Modified : {2}"
    CHANGED_TO_EXCLUSIVELOCK = "Line {0} | Operation : Released Shared Lock and Given Exclusive Lock for Lock Table Entry | Additional: Added Item to Transaction's LockedItems field  | Tid Added : {1}  | Item Modified : {2}"
    WAIT_DIE_PERFORMED = "Line {0} | Operation: Perform Wait Die Between Old Transaction and New Transaction | Old Transaction Tid : {1} | New Transaction Tid : {2} | Output : New Transaction Tid  {2} is {3}"
    WRITE_GROWING =  "Line {0} | Operation : Changing  Shared Lock to Exclusive Lock for Lock Table Entry | Tid  : {1}  | Item Modified : {2}"
    CREATE_LOCK_ENTRY = "Line {0} | Operation : Added Lock Entry to Lock Table | Additional: Added  Item to Transaction's LockedItems field| Tid : {1} | Item : {2} "
    COMMIT_TRANSACTIONS = "Line {0} | Operation : Commited Transaction Entry and Released Locks | Tid : {1} | Items Locks Released : {2} "
    BLOCKED_TRANSACTION = "Line {0} | Operation : Transaction Already Blocked, Adding (Operations , Item) to WaitingOperations List in Transaction Entry and Adding Transaction to WaitingTransaction List in Lock Entry | Tid : {1} | Item : {2} "
    ABORTED_TRANSACTION = "Line {0}  | Operation : Transaction Already Aborted, Ignoring Operation | Tid {1}"
    BLOCKING_TRANSACTION = "Line {0} | Operation :  Blocking Transaction, Adding (Operation , Item) to WaitingOperations List in Transaction Entry and Adding Transaction to WaitingTransaction List in Lock Entry | Tid : {1} | Item : {2} "
    ABORTING_TRANSACTION = "Line {0}  | Operation : Aborting Transactions, Ignoring Operation | Tid {1}"
    WAIT_DIE_TWRITE_LREAD = "Line {0} | Operation : Transaction {1} wants to Write but Item {2} has SharedLock with Multiple Transactions Reading."










def start():
    global lock_table
    global transaction_table
    global currLine
    with open('files.txt','r') as file:
        for line in file:
            line = line.replace("\n","").strip()
            currLine = line
            output = re.search(regex, line)
            if output:
                operation = output.group(1)
                transactionID = output.group(2)
                item = output.group(3)
            else:
                continue
            if (operation == 'Input'):
                printList.append("\n\n Output")
                counter = 0
                transaction_table = {}
                lock_table = {}
            else:
                counter += 1
                if operation == 'b':
                    transaction_table[transactionID] = {"Timestamp": counter, "TState":TranscState.ACTIVE, "LockedItems": [],
                                                        "WaitingOperations": [], "isGrowing": True}
                    printList.append(str(PrintActions.CREATE_TRANSACTION_ENTRY).format(line,transactionID))

                elif transactionID in transaction_table:
                        if (transaction_table[transactionID]['TState'] == TranscState.ACTIVE):
                            processOperation[operation](operation,transactionID,item,line)
                        elif (transaction_table[transactionID]['TState'] == TranscState.BLOCKED):
                            transaction = transaction_table[transactionID]
                            if (operation == 'r' or operation == 'w'):
                                lock_entry = lock_table[item]
                                transaction['WaitingOperations'].append({'operation': operation, 'item': item})
                                lock_entry['WaitingTransactions'].append(transactionID)
                                printList.append(str(PrintActions.BLOCKED_TRANSACTION).format(line,transactionID,item))
                            elif operation == 'e':
                                printList.append(str(PrintActions.BLOCKED_TRANSACTION).format(line, transactionID, "N/A"))
                        elif (transaction_table[transactionID]['TState'] == TranscState.ABORTED):
                            printList.append(str(PrintActions.ABORTED_TRANSACTION).format(currLine, transactionID))
                            pass
        print(transaction_table)
        print(lock_table)







def read(operation,transactionID,item,line):
    global lock_table
    global transaction_table
    current_transaction = transaction_table[transactionID]
    if current_transaction['isGrowing'] == True:
        if item in lock_table:
            lock_entry = lock_table[item]
            if(lock_entry['LockState'] == LockState.SHAREDLOCK):
                if transactionID not in lock_entry['Transactions']:
                    printList.append(str(PrintActions.READ_APPEND_SHAREDLOCK).format(line,operation,item))
                    lock_entry['Transactions'].append(transactionID)
                    current_transaction['LockedItems'].append(item)

            elif(lock_entry['LockState'] == LockState.EXCLUSIVELOCK):
                if len(lock_entry['Transactions']) == 0:
                    lock_entry['Transactions'].append(transactionID)
                    lock_entry['LockState'] = LockState.SHAREDLOCK
                    current_transaction['LockedItems'].append(item)
                    printList.append(str(PrintActions.CHANGED_TO_SHAREDLOCK).format(line,transactionID,item))

                elif transactionID not in lock_entry['Transactions']:
                    # lock_entry['Transactions'].append(transactionID)
                    old_transc_id = lock_entry['Transactions'][0]
                    result = performWaitDie(old_transc_id,transactionID,operation,item,lock_entry)



        else:
            lock_table[item] = {'LockState' : LockState.SHAREDLOCK , 'Transactions' : [transactionID] , 'WaitingTransactions':[] }
            current_transaction['LockedItems'].append(item)
            printList.append(str(PrintActions.CREATE_LOCK_ENTRY).format(line,transactionID,item))




def write(operation,transactionID,item,line):
    global lock_table
    global transaction_table
    current_transaction = transaction_table[transactionID]
    if current_transaction['isGrowing'] == True:
        if item in lock_table:
            lock_entry = lock_table[item]
            if(lock_entry['LockState'] == LockState.SHAREDLOCK):
                if(len(lock_entry['Transactions']) == 1 and lock_entry['Transactions'][0] == transactionID):
                    lock_entry['LockState'] = LockState.EXCLUSIVELOCK
                    printList.append(str(PrintActions.WRITE_GROWING).format(line, transactionID, item))
                else:
                    printList.append(str(PrintActions.WAIT_DIE_TWRITE_LREAD).format(line,transactionID,item))
                    for old_transaction in lock_entry['Transactions']:
                        result = performWaitDie(old_transaction, transactionID, operation, item, lock_entry)

                        if result == TranscState.ABORTED:
                            break;

            elif(lock_entry['LockState'] == LockState.EXCLUSIVELOCK):
                if transactionID not in lock_entry['Transactions']:
                    # lock_entry['Transactions'].append(transactionID)
                    old_transc_id = lock_entry['Transactions'][0]
                    result = performWaitDie(old_transc_id,transactionID,operation,item,lock_entry)




        else:
            lock_table[item] = {'LockState' : LockState.SHAREDLOCK , 'Transactions' : [transactionID] , 'WaitingTransactions':[] }
            current_transaction['LockedItems'].append(item)
            printList.append(str(PrintActions.CREATE_LOCK_ENTRY).format(line, transactionID, item))


def commit(operation,transactionID,item,line):
    printList.append(str(PrintActions.COMMIT_TRANSACTIONS).format(line, transactionID, transaction_table[transactionID]['LockedItems']))
    finalizeTransaction(transactionID, TranscState.COMMITED)

processOperation = { 'r' : read,'w':write,'e':commit}



def performWaitDie(old_tid,new_tid,new_trans_op,new_trans_item,lock_entry):
    global lock_table
    global transaction_table
    old_trans = transaction_table[old_tid]
    new_trans = transaction_table[new_tid]
    if old_tid == new_tid :
        return "Same-Entry"
    if(new_trans['TState'] != TranscState.BLOCKED or new_trans['TState'] != TranscState.ABORTED):
        if(new_trans['Timestamp'] < old_trans['Timestamp']):
            new_trans['TState'] = TranscState.BLOCKED
            new_trans['WaitingOperations'].append({'operation':new_trans_op , 'item' : new_trans_item})
            lock_entry['WaitingTransactions'].append(new_tid)
            printList.append(str(PrintActions.BLOCKING_TRANSACTION).format(currLine, new_tid, new_trans_item))
            return TranscState.BLOCKED
        else:
            finalizeTransaction(new_tid,TranscState.ABORTED)
            printList.append(str(PrintActions.ABORTING_TRANSACTION).format(currLine, new_tid))
            return TranscState.ABORTED

def finalizeTransaction(transactionID,tstate):
    global lock_table
    global transaction_table
    current_transaction = transaction_table[transactionID]
    current_transaction['TState'] = tstate
    current_transaction['isGrowing'] = False

    for item in current_transaction['LockedItems']:
        lock_entry = lock_table[item]
        lock_entry['Transactions'].remove(transactionID)

        if len(lock_entry['WaitingTransactions']) == 0:
            continue

        firstTid = lock_entry['WaitingTransactions'].pop(0)
        firstTransaction = transaction_table[firstTid]
        firstEntry = firstTransaction['WaitingOperations'].pop(0)

        if(firstEntry['operation'] == 'r'):
            # if lock_entry['LockStatus'] == LockState.SHAREDLOCK:
            #     if firstTid not in lock_entry['Transactions']:
            #         lock_entry['Transactions'].append(firstTid)
            #         firstTransaction['LockedItems'].append(item)
            #         firstTransaction['TState'] = TranscState.ACTIVE
            if lock_entry['LockState'] == LockState.EXCLUSIVELOCK:
                lock_entry['LockState'] = LockState.SHAREDLOCK
                lock_entry['Transactions'] = [firstTid]
                firstTransaction['TState'] = TranscState.ACTIVE
                for tid in lock_entry['WaitingTransactions']:
                    next_transaction =  transaction_table[tid]['WaitingOperations'][0]
                    if next_transaction['operation'] == 'r':
                        transaction_table[tid]['WaitingOperations'].pop(0)
                        lock_entry['WaitingTransactions'].remove(tid)
                        transaction_table[tid]['TState'] = TranscState.ACTIVE
                        lock_entry['Transactions'].append(tid)
        elif(firstEntry['operation'] == 'w'):
            if lock_entry['LockState'] == LockState.SHAREDLOCK:
                if len(lock_entry['Transactions']) == 0 or lock_entry['Transactions'][0] == firstTid:
                    lock_entry['LockState'] = LockState.EXCLUSIVELOCK
                    lock_entry['Transactions'] = [firstTid]
                    firstTransaction['TState'] = TranscState.ACTIVE
                elif len(lock_entry['Transactions'] > 0):
                    for old_transaction in lock_entry['Transactions']:
                        result = performWaitDie(old_transaction, firstTid,firstEntry['operation'], firstEntry['item'], lock_entry)
                        if result == TranscState.ABORTED:
                            break;

            elif lock_entry['LockState'] == LockState.EXCLUSIVELOCK:
                lock_entry['Transactions'] = [firstTid]
                firstTransaction['TState'] = TranscState.ACTIVE
    current_transaction['LockedItems'] = []







start()
print(printList)

with open('op.txt','w') as f:
    for x in printList:
        f.write(x)
        f.write("\n")