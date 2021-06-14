#include <iostream>
#include <algorithm>
#include <string>
#include <fstream>
#include <vector>
#include <cassert>
#include <iomanip>
#include <numeric>

int timestamp_intializer = 0;

class Action;
struct Lock;

typedef std::vector<Action> schedule_t;

class Action
{
public:
    Action(std::string obj, std::string trans, std::string type) : obj_(obj), transaction_(trans), type_(type) {}

    std::string GetTransaction() const { return transaction_; }
    std::string GetObject() const { return obj_; }
    std::string GetType() const { return type_; }

    friend std::ostream& operator<<(std::ostream& os, const Action& action)
    {
        os << "Action(" << action.obj_ << ", " << action.transaction_ << ", " << action.type_ << ")" << std::endl;
        return os;
    }

    friend bool operator==(const Action& a, const Action& b)
    {
        return a.obj_ == b.obj_ && a.transaction_ == b.transaction_ && a.type_ == b.type_;
    }

private:
    std::string obj_;
    std::string transaction_;
    std::string type_;
};

struct Transaction
{
    Transaction(const std::string& name) : name_(name)
    {
        timestamp_ = timestamp_intializer;
        timestamp_intializer++;
    }

    friend bool operator==(const Transaction& a, const Transaction& b)
    {
        return a.name_ == b.name_;
    }

    std::string name_;
    int timestamp_;
    schedule_t queue_;
    std::vector<Lock> waiting_;
    bool wait_ = false;
};

struct Lock
{
    friend bool operator==(const Lock& a, const Lock& b)
    {
        return a.transaction_ == b.transaction_ && a.obj_ == b.obj_;
    }

    Lock(const std::string& transaction, const std::string& object)
    {
        transaction_ = transaction;
        obj_ = object;
    }

    std::string transaction_;
    std::string obj_;
};

schedule_t WaitDieScheduler(const schedule_t& toschedule);
bool TransactionInPool(const std::vector<Transaction>& pool, const Action& action);
void SyncLocks(std::vector<Transaction>& waitpool, std::vector<Lock> locks);
bool TransactionsInQueues(const std::vector<Transaction>& pool);
bool LockExists(const std::string& obj, const std::vector<Lock>& locks);
bool TransactionOwnsLock(const std::string& transaction, const std::string& obj, const std::vector<Lock>& locks);
Transaction GetLockOwner(const std::vector<Transaction>& transactions, const std::string& obj, const std::vector<Lock>& locks);
int ProcessAction(const schedule_t& toschedule, schedule_t& schedule, std::vector<Lock>& locks, bool& aflag, std::vector<Transaction>& pool, Transaction& current, int actioni);

void Print(std::vector<Lock>& locks, std::vector<Transaction>& pool);

int main()
{
    Action a("A", "pear", "WRITE");
    Action b("NA", "pear", "COMMIT");
    schedule_t test_one{ a, b };

    Action c("A", "pear",  "WRITE");
    Action d("B", "apple",  "WRITE");
    Action e("B", "pear",  "WRITE");
    Action f("A", "apple", "WRITE");
    Action g("NA", "apple",  "COMMIT");
    Action h("NA", "pear",  "COMMIT");
    schedule_t test_two {c, d, e, f, g, h};

    Action i("A", "pear", "WRITE");
    Action j("B", "apple",  "WRITE");
    Action k("C", "carrot", "WRITE");
    Action l("B", "pear", "WRITE");
    Action m("A", "apple",  "WRITE");
    Action n("NA", "apple", "COMMIT");
    Action o("B", "carrot", "WRITE");
    Action p("NA", "pear",  "COMMIT");
    Action q("NA", "carrot",  "COMMIT");
    schedule_t test_three{ i, j, k, l, m, n, o, p, q };

    auto schedule = WaitDieScheduler(test_three);

    /*for (auto i : schedule)
    {
        std::cout << i << std::endl;
    }*/

    return 0;
}

schedule_t WaitDieScheduler(const schedule_t& toschedule)
{
    // Transactions actively happening
    std::vector<Transaction> pool;

    // All locks currently held
    std::vector<Lock> locks;

    // Schedule to be created
    schedule_t schedule;

    // Action index
    int actioni = 0;

    bool done = false;
    while (!done)
    {
        // Sort the active transactions in the pool by name
        std::sort(pool.begin(), pool.end(), [](const Transaction& lhs, const Transaction& rhs) {
            return lhs.name_ < rhs.name_;
            });

        for (auto& t : pool)
        {
            bool action_done = false; // flag that will check if an operation was 'completed'
            if (t.queue_.size() > 0)
            {
                std::cout << std::setfill('-') << std::setw(20) << "\n" << std::endl;
                ProcessAction(toschedule, schedule, locks, action_done, pool, t, actioni);
                Print(locks, pool);
            }
            
            if (action_done)
            {
                // if action was completed, remove it from queue
                assert(!t.queue_.empty());
                t.queue_.erase(t.queue_.begin());
            }

            // Make sure transactions in waitpool are not waiting on locks that have been released
            SyncLocks(pool, locks);

            // Return transactions to pool if they are not waiting any longer
            for (auto& i : pool)
            {
                if (i.waiting_.size() == 0 && i.wait_)
                {
                    i.wait_ = false;
                }
            }
        }

        if (actioni < toschedule.size())
        {
            // next transaction to be added
            auto ttoadd = toschedule[actioni];
            if (!TransactionInPool(pool, ttoadd))
            {
                Transaction t(ttoadd.GetTransaction());
                t.queue_.push_back(ttoadd);
                pool.push_back(t);
            }
            else
            {
                for (auto& transaction : pool)
                {
                    if (transaction.name_ == ttoadd.GetTransaction())
                    {
                        transaction.queue_.push_back(ttoadd);
                        break;
                    }
                }
            }
            
            actioni++;
        }

        if (!TransactionsInQueues(pool))
        {
            done = true;
        }
    }

    return schedule;
}

bool TransactionInPool(const std::vector<Transaction>& pool, const Action& action)
{
    for (const auto& i : pool)
    {
        if (i.name_ == action.GetTransaction())
        {
            return true;
        }
    }

    return false;
}

void SyncLocks(std::vector<Transaction>& pool, std::vector<Lock> locks)
{   
    for (auto& tran : pool)
    {     
        if (tran.wait_)
        {
            // remove all locks from this transactions waiting list if the lock is no
            //      longer active
            std::vector<Lock> newlocks;
            for (const auto& lock : tran.waiting_)
            {
                auto pos = std::find(locks.begin(), locks.end(), lock);
                if (pos != locks.end())
                {
                    newlocks.push_back(lock);
                }
            }
            tran.waiting_ = newlocks;
        }
    }
}

bool TransactionsInQueues(const std::vector<Transaction>& pool)
{
    bool ret = false;

    for (const auto& i : pool)
    {
        if (i.queue_.size() > 0)
        {
            ret = true;
            break;
        }
    }

    return ret;
}

bool LockExists(const std::string& obj, const std::vector<Lock>& locks)
{
    bool found = false;

    for (const auto & i : locks)
    {
        if (i.obj_ == obj)
        {
            found = true;
            break;
        }
    }

    return found;
}

bool TransactionOwnsLock(const std::string& transaction, const std::string& obj, const std::vector<Lock>& locks)
{
    bool found = false;
    for (const auto& i : locks)
    {
        if (i.obj_ == obj && i.transaction_ == transaction)
        {
            found = true;
            break;
        }
    }
    
    return found;
}

Transaction GetLockOwner(const std::vector<Transaction>& transactions, const std::string& obj, const std::vector<Lock>& locks)
{
    std::string owner;
    // Find the owner of the lock this object is in
    auto f = std::find_if(locks.begin(), locks.end(), [obj](const Lock& l) {
        return l.obj_ == obj; });

    owner = f->transaction_;

    // Find the transaction that owned the lock
    auto found = std::find_if(transactions.begin(), transactions.end(), [owner](const Transaction& t) {
        return t.name_ == owner; });
    
    return *found;
}

int ProcessAction(const schedule_t& toschedule, schedule_t& schedule, std::vector<Lock>& locks, bool& aflag, std::vector<Transaction>& pool, Transaction& current, int actioni)
{
    // If there are items in queue, grab the first one
    if (!current.wait_)
    {
        auto action = current.queue_.at(0);

        if (action.GetType() == "WRITE")
        {
            if (LockExists(action.GetObject(), locks))
            {
                if (TransactionOwnsLock(action.GetTransaction(), action.GetObject(), locks))
                {
                    schedule.push_back(action);
                    aflag = true;
                }
                else
                {
                    // this transaction needs to either wait or die
                    auto owner = GetLockOwner(pool, action.GetObject(), locks);

                    if (current.timestamp_ < owner.timestamp_)
                    {
                        schedule.push_back(Action(action.GetObject(), action.GetTransaction(), "WAIT"));

                        Lock l(owner.name_, action.GetObject());

                        // This transaction is now waiting on lock l
                        current.waiting_.push_back(l);

                        current.wait_ = true;
                    }
                    else
                    {
                        schedule.push_back(Action(action.GetObject(), action.GetTransaction(), "ROLLBACK"));

                        current.queue_.clear();
                        current.waiting_.clear();

                        for (int i = 0; i < actioni; i++)
                        {
                            if (toschedule[i].GetTransaction() == current.name_)
                            {
                                current.queue_.push_back(toschedule[i]);
                            }
                        }

                        std::vector<Lock> toremove;
                        for (auto i : locks)
                        {
                            if (i.transaction_ == current.name_)
                            {
                                std::cout << "LOCK RELEASED BY: " << current.name_ << "; ";
                                std::cout << i.obj_ << " ";
                                toremove.push_back(i);
                                schedule.push_back(Action(i.obj_, i.transaction_, "UNLOCK"));
                            }
                        }

                        std::cout << std::endl;

                        for (auto& i : toremove)
                        {
                            locks.erase(std::remove(locks.begin(), locks.end(), i), locks.end());
                        }
                    }

                    aflag = false;
                }
            }
            else
            {
                schedule.push_back(Action(action.GetObject(), action.GetTransaction(), "LOCK"));
                schedule.push_back(action);

                Lock k(action.GetTransaction(), action.GetObject());
                locks.push_back(k);

                aflag = true;
            }
        }
        else if (action.GetType() == "COMMIT")
        {
            schedule.push_back(action);

            std::vector<Lock> toremove;
            for (auto i : locks)
            {
                if (i.transaction_ == action.GetTransaction())
                {
                    toremove.push_back(i);
                    schedule.push_back(Action(i.obj_, i.transaction_, "UNLOCK"));
                }
            }

            for (auto i : toremove)
            {
                locks.erase(std::remove(locks.begin(), locks.end(), i), locks.end());
            }

            aflag = true;
        }
    }

    return 0;
}

void Print(std::vector<Lock>& locks, std::vector<Transaction>& pool)
{
    for (auto i : locks)
    {
        std::cout << "Lock: " << i.obj_ << "; Owned by: " << i.transaction_ << "; Being waited on by: ";
        for (auto j : pool)
        {
            for (auto k : j.waiting_)
            {
                if (k.obj_ == i.obj_)
                {
                    std::cout << j.name_ << " ";
                }
            }
        }

        std::cout << "\n\n";
    }

    std::cout << std::setfill('-') << std::setw(20) << "";

    std::cout << "\n\n\n" << std::endl;
}