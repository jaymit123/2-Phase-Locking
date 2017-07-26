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


#PrintAction Enum : to print if any changes in the lock table or transaction table and other important changes
class PrintActions(Enum):
    def __str__(self):
        return str(self.value)

    CREATE_TRANSACTION_ENTRY = "Line {0} | Operation : Added Transaction Entry to Transaction Table| Tid : {1}"
    READ_APPEND_SHAREDLOCK = "Line {0} | Operation : Added Transaction to Shared Lock for Lock Table Entry | Additional: Added Item to Transaction's LockedItems field | Tid Added : {1}  | Item Modified : {2}"
    CHANGED_TO_SHAREDLOCK = "Line {0} | Operation : Given Shared Lock for Lock Table Entry | Additional: Added Item to Transaction's LockedItems field  | Tid Added : {1}  | Item Modified : {2}"
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
    ABORTING_GIVING_SHAREDLOCK = "Line {0} | Operation: Transaction {1} Released Exclsuive Lock on Item {2}, Transaction {3} given Shared Lock on Item {2}"
    ABORTING_APPENDING_SHAREDLOCK = "Line {0} | Operation: Transaction {1} Given  Shared Lock on Item {2} "
    ABORTING_GIVING_EXCLUSIVELOCK = "Line {0} | Operation: Transaction {1} Released SharedLock on Item {2}, Transaction {3} given ExclusiveLock on Item {2}"

# start () : checks if operation is begin or any other operation. if Transaction on which operation needs to be performed is in active state then perform operation r,w,e
#     It also handles other Transaction States. If the Transaction is blocked we add the operation into that Transactions WaitingOperations list,
#     if that Transaction is  aborted we ignore the operations
def start():
    global lock_table
    global transaction_table
    global currLine
    with open('files.txt','r') as file:
        for line in file:
            line = line.replace("\n","").strip()
            currLine = line
            output = re.search(regex, line)       #regex search method helps to seperate the transaction id, operation and item name from the line.
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
                #if begin operation is read from line, create a new transaction in transaction table
                if operation == 'b':
                    transaction_table[transactionID] = {"Timestamp": counter, "TState":TranscState.ACTIVE, "LockedItems": [],
                                                        "WaitingOperations": [], "isGrowing": True}
                    printList.append(str(PrintActions.CREATE_TRANSACTION_ENTRY).format(line,transactionID))
                #otherwise if transaction exist in transaction table check its state and accordingly perform operation
                elif transactionID in transaction_table:
                        #if transaction is in ACTIVE state
                        if (transaction_table[transactionID]['TState'] == TranscState.ACTIVE):
                            processOperation[operation](operation,transactionID,item,line)
                        # if transaction is in BLOCKED state
                        elif (transaction_table[transactionID]['TState'] == TranscState.BLOCKED):
                            transaction = transaction_table[transactionID]
                            if (operation == 'r' or operation == 'w'):
                                lock_entry = lock_table[item]
                                transaction['WaitingOperations'].append({'operation': operation, 'item': item})
                                lock_entry['WaitingTransactions'].append(transactionID)
                                printList.append(str(PrintActions.BLOCKED_TRANSACTION).format(line,transactionID,item))
                            elif operation == 'e':
                                transaction['WaitingOperations'].append({'operation': operation, 'item': "N/A"})
                                printList.append(str(PrintActions.BLOCKED_TRANSACTION).format(line, transactionID, "N/A"))
                        # if transaction is in ABORT state
                        elif (transaction_table[transactionID]['TState'] == TranscState.ABORTED):
                            printList.append(str(PrintActions.ABORTED_TRANSACTION).format(currLine, transactionID))
                            pass






#read () : handles situation if transaction in Active State, wants to Read an Item
def read(operation,transactionID,item,line):
    global lock_table
    global transaction_table
    current_transaction = transaction_table[transactionID]
    if current_transaction['isGrowing'] == True:
        if item in lock_table:
            lock_entry = lock_table[item]
            #if current transaction wants to read and item LockState is SharedLock
            if(lock_entry['LockState'] == LockState.SHAREDLOCK):
                if transactionID not in lock_entry['Transactions']:
                    printList.append(str(PrintActions.READ_APPEND_SHAREDLOCK).format(line,operation,item))
                    lock_entry['Transactions'].append(transactionID)
                    current_transaction['LockedItems'].append(item)


            # if current transaction wants to read and item LockState is Exclusive Lock
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
            #if item not in lock table create a new entry
            lock_table[item] = {'LockState' : LockState.SHAREDLOCK , 'Transactions' : [transactionID] , 'WaitingTransactions':[] }
            current_transaction['LockedItems'].append(item)
            printList.append(str(PrintActions.CREATE_LOCK_ENTRY).format(line,transactionID,item))



#write () : handles situation if transaction in Active State and wants a ExclusiveLock on Item
def write(operation,transactionID,item,line):
    global lock_table
    global transaction_table
    current_transaction = transaction_table[transactionID]
    if current_transaction['isGrowing'] == True:
        if item in lock_table:
            lock_entry = lock_table[item]
            # if current transaction wants to write and item LockState is SharedLock
            if(lock_entry['LockState'] == LockState.SHAREDLOCK):

                # Handles Upgrading of LockState to ExclusiveLock ( Growing ) since same Transaction wants a Lock on item
                if(len(lock_entry['Transactions']) == 1 and lock_entry['Transactions'][0] == transactionID):
                    lock_entry['LockState'] = LockState.EXCLUSIVELOCK
                    printList.append(str(PrintActions.WRITE_GROWING).format(line, transactionID, item))
                else:
                # if many transactions have SharedLock on item and current transaction wants a write lock.
                    printList.append(str(PrintActions.WAIT_DIE_TWRITE_LREAD).format(line,transactionID,item))
                    for old_transaction in lock_entry['Transactions']:
                        result = performWaitDie(old_transaction, transactionID, operation, item, lock_entry)

                        if result == TranscState.ABORTED:
                            break;

            # if currents transaction wants to write and item is in ExclusiveLock State
            elif(lock_entry['LockState'] == LockState.EXCLUSIVELOCK):
                if transactionID not in lock_entry['Transactions']:
                    # lock_entry['Transactions'].append(transactionID)
                    old_transc_id = lock_entry['Transactions'][0]
                    result = performWaitDie(old_transc_id,transactionID,operation,item,lock_entry)




        else:
            # if item not in lock table create a new entry
            lock_table[item] = {'LockState' : LockState.SHAREDLOCK , 'Transactions' : [transactionID] , 'WaitingTransactions':[] }
            current_transaction['LockedItems'].append(item)
            printList.append(str(PrintActions.CREATE_LOCK_ENTRY).format(line, transactionID, item))


# handles the situation if we read a 'e' operation or transaction wants to commit.
def commit(operation,transactionID,item,line):
    printList.append(str(PrintActions.COMMIT_TRANSACTIONS).format(line, transactionID, transaction_table[transactionID]['LockedItems']))
    finalizeTransaction(transactionID, TranscState.COMMITED)


#processOperation: This Dictionary is Invoked whenever we read a new line and transaction present is in Active State.
processOperation = { 'r' : read,'w':write,'e':commit}


# performWaitDie() : Performs the wait die algorithm, if Transaction is blocked, we push it into Waiting Transactions LIst for that item in Lock Table and
#     Add the current operation to waiting operation list in that Transactions.
#     If Transaction is aborted we release all locks held by that Transaction from the LockTable and Set its state to aborted

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
            printList.append(str(PrintActions.ABORTING_TRANSACTION).format(currLine, new_tid))
            finalizeTransaction(new_tid,TranscState.ABORTED)
            return TranscState.ABORTED



 # finalizeTransaction(): Since both Commit and Abort have the same logic 1 method is used, The difference is the state of Transaction is set to Aborted or Commited based on the situation.
 #                           IN Abort/Commit we pop the first value from the LinkedLIst of Waiting Transactions and Then if That waiting transaction wants to perform Read we POp all other Transactions
 #                           from the Waiting Transactions List which want to read and set their states to active and set the items Lock State to "Shared-Lock".
 #                           If its write we just pop it from the Waiting Transaction List and then set its state to active and set the items Lock State to "Exclusive-Lock"
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

        if firstEntry['item'] != item:
            continue


        if(firstEntry['operation'] == 'r' ):
            # if lock_entry['LockStatus'] == LockState.SHAREDLOCK:
            #     if firstTid not in lock_entry['Transactions']:
            #         lock_entry['Transactions'].append(firstTid)
            #         firstTransaction['LockedItems'].append(item)
            #         firstTransaction['TState'] = TranscState.ACTIVE
            if lock_entry['LockState'] == LockState.EXCLUSIVELOCK:
                lock_entry['LockState'] = LockState.SHAREDLOCK
                lock_entry['Transactions'] = [firstTid]
                firstTransaction['TState'] = TranscState.ACTIVE
                printList.append(str(PrintActions.ABORTING_GIVING_SHAREDLOCK).format(currLine,transactionID,item,firstTid))
                checkWaitingOperation(firstTid)
                for tid in lock_entry['WaitingTransactions']:
                    transaction_iterator =  transaction_table[tid]['WaitingOperations'][0]
                    if transaction_iterator['operation'] == 'r':
                        transaction_table[tid]['WaitingOperations'].pop(0)
                        lock_entry['WaitingTransactions'].remove(tid)
                        transaction_table[tid]['TState'] = TranscState.ACTIVE
                        lock_entry['Transactions'].append(tid)
                        printList.append(str(PrintActions.ABORTING_APPENDING_SHAREDLOCK).format(currLine, tid, item))
                        checkWaitingOperation(tid)

        elif(firstEntry['operation'] == 'w'):
            if lock_entry['LockState'] == LockState.SHAREDLOCK:
                if len(lock_entry['Transactions']) == 0 or  (len(lock_entry['Transactions']) == 1 and lock_entry['Transactions'][0] == firstTid):
                    lock_entry['LockState'] = LockState.EXCLUSIVELOCK
                    lock_entry['Transactions'] = [firstTid]
                    firstTransaction['TState'] = TranscState.ACTIVE
                    printList.append(str(PrintActions.ABORTING_GIVING_EXCLUSIVELOCK).format(currLine, transactionID, item, firstTid))
                    checkWaitingOperation(firstTid)
                elif len(lock_entry['Transactions'] > 0):
                    for old_transaction in lock_entry['Transactions']:
                        result = performWaitDie(old_transaction, firstTid,firstEntry['operation'], firstEntry['item'], lock_entry)
                        if result == TranscState.ABORTED:
                            break;

            elif lock_entry['LockState'] == LockState.EXCLUSIVELOCK:
                lock_entry['Transactions'] = [firstTid]
                firstTransaction['TState'] = TranscState.ACTIVE
                printList.append(str(PrintActions.ABORTING_GIVING_EXCLUSIVELOCK).format(currLine, transactionID, item, firstTid))
                checkWaitingOperation(firstTid)
    current_transaction['LockedItems'] = []


# checkWaitingOperation: used in finalizeTransaction method, called to check if current transactions next WaitingOperation is 'e' i.e Commit
def checkWaitingOperation(tid):
    if (transaction_table[tid]['WaitingOperations'][0]['operation'] == 'e'):
        operation = transaction_table[tid]['WaitingOperations'].pop(0)
        printList.append(str(PrintActions.COMMIT_TRANSACTIONS).format("e"+tid, tid,transaction_table[tid]['LockedItems']))
        finalizeTransaction(tid, TranscState.COMMITED)



start()

with open('op.txt','w') as f:
    for x in printList:
        f.write(x)
        f.write("\n")