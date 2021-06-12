#include <iostream>
#include <algorithm>
#include <string>
#include <fstream>
#include <vector>
#include <cassert>

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
};

struct Lock
{
    friend bool operator==(const Lock& a, const Lock& b)
    {
        return a.transaction_ == b.transaction_ && a.obj_ == b.obj_;
    }

    std::string transaction_;
    std::string obj_;
};

schedule_t WaitDieScheduler(const schedule_t& toschedule);
bool TransactionInPool(const std::vector<Transaction>& pool, const std::vector<Transaction>& waitpool, const Action& action);
void SyncLocks(std::vector<Transaction>& waitpool, std::vector<Lock> locks);
bool TransactionsInQueues(const std::vector<Transaction>& pool);
bool LockExists(const std::string& obj, const std::vector<Lock>& locks);
bool TransactionOwnsLock(const std::string& transaction, const std::string& obj, const std::vector<Lock>& locks);
Transaction GetLockOwner(std::vector<Transaction> transactions, const std::string& obj, const std::vector<Lock>& locks);

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

    for (auto i : schedule)
    {
        std::cout << i << std::endl;
    }

    return 0;
}

schedule_t WaitDieScheduler(const schedule_t& toschedule)
{
    // Transactions actively happening
    std::vector<Transaction> pool;

    // Transactions waiting to happen
    std::vector<Transaction> waitpool;

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
            return lhs.name_ > rhs.name_;
            });

        // storage for transactions we'll remove if theyre told to wait
        std::vector<Transaction> remove_from_pool;
        for (auto& t : pool)
        {
            bool action_done = false;
            if (t.queue_.size() > 0)
            {
                // If there are items in queue, grab the first one
                auto action = t.queue_.at(0);

                if (action.GetType() == "WRITE")
                {
                    if (LockExists(action.GetObject(), locks))
                    {
                        if (TransactionOwnsLock(action.GetTransaction(), action.GetObject(), locks))
                        {
                            schedule.push_back(action);
                            action_done = true;
                        }
                        else
                        {
                            std::vector<Transaction> pooltosearch;
                            for (auto i : pool)
                            {
                                pooltosearch.push_back(i);
                            }

                            for (auto i : waitpool)
                            {
                                pooltosearch.push_back(i);
                            }

                            // this transaction needs to either wait or die
                            auto owner = GetLockOwner(pooltosearch, action.GetObject(), locks);

                            if (t.timestamp_ < owner.timestamp_)
                            {
                                schedule.push_back(Action(action.GetObject(), action.GetTransaction(), "WAIT"));
                                
                                Lock l;
                                l.obj_ = action.GetObject();
                                l.transaction_ = owner.name_;

                                // This transaction is now waiting on lock l
                                t.waiting_.push_back(l);

                                // Move to wait pool
                                remove_from_pool.push_back(t);
                                waitpool.push_back(t);
                            }
                            else
                            {
                                schedule.push_back(Action(action.GetObject(), action.GetTransaction(), "ROLLBACK"));

                                t.queue_.clear();
                                t.waiting_.clear();

                                for (int i = 0; i < actioni; i++)
                                {
                                    if (toschedule[i].GetTransaction() == t.name_)
                                    {
                                        t.queue_.push_back(toschedule[i]);
                                    }
                                }

                                std::vector<Lock> toremove;
                                for (auto i : locks)
                                {
                                    if (i.transaction_ == t.name_)
                                    {
                                        toremove.push_back(i);
                                        schedule.push_back(Action(i.obj_, i.transaction_, "UNLOCK"));
                                    }
                                }

                                for (auto i : toremove)
                                {
                                    locks.erase(std::remove(locks.begin(), locks.end(), i), locks.end());
                                }
                            }
                                
                            action_done = false;
                        }
                    }
                    else
                    {
                        schedule.push_back(Action(action.GetObject(), action.GetTransaction(), "LOCK"));
                        schedule.push_back(action);

                        Lock k;
                        k.obj_ = action.GetObject();
                        k.transaction_ = action.GetTransaction();
                        locks.push_back(k);

                        action_done = true;
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

                    action_done = true;
                }
            }

            if (action_done)
            {
                // if action was completed, remove it from queue
                assert(!t.queue_.empty());
                t.queue_.erase(t.queue_.begin());
            }

            // Make sure transactions in waitpool are not waiting on locks that have been released
            SyncLocks(waitpool, locks);

            // Return transactions to pool if they are not waiting any longer
            std::vector<Transaction> waitpoolremove;
            for (auto i : waitpool)
            {
                if (i.waiting_.size() == 0)
                {
                    pool.push_back(i);
                    waitpoolremove.push_back(i);
                }
            }

            // remove restarted transaction from waitpool 
            for (auto transaction : waitpoolremove)
            {
                waitpool.erase(std::remove(waitpool.begin(), waitpool.end(), transaction), waitpool.end());
            }
        }

        for (auto i : remove_from_pool)
        {
            pool.erase(std::remove(pool.begin(), pool.end(), i), pool.end());
        }

        if (actioni < toschedule.size())
        {
            // next transaction to be added
            auto ttoadd = toschedule[actioni];
            if (!TransactionInPool(pool, waitpool, ttoadd))
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

                for (auto& transaction : waitpool)
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

bool TransactionInPool(const std::vector<Transaction>& pool, const std::vector<Transaction>& waitpool, const Action& action)
{
    for (const auto& i : pool)
    {
        if (i.name_ == action.GetTransaction())
        {
            return true;
        }
    }

    for (const auto& i : waitpool)
    {
        if (i.name_ == action.GetTransaction())
        {
            return true;
        }
    }

    return false;
}

void SyncLocks(std::vector<Transaction>& waitpool, std::vector<Lock> locks)
{
    for (auto& tran : waitpool)
    {
        std::vector<Lock> newlocks;
        for (auto& lock : tran.waiting_)
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

bool TransactionsInQueues(const std::vector<Transaction>& pool)
{
    bool ret = false;

    for (auto i : pool)
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

    for (auto i : locks)
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
    for (auto i : locks)
    {
        if (i.obj_ == obj && i.transaction_ == transaction)
        {
            found = true;
            break;
        }
    }
    
    return found;
}

Transaction GetLockOwner(std::vector<Transaction> transactions, const std::string& obj, const std::vector<Lock>& locks)
{
    std::string owner;
    for (auto i : locks)
    {
        if (obj == i.obj_)
        {
            owner = i.transaction_;
        }
    }

    for (auto& i : transactions)
    {
        if (i.name_ == owner)
        {
            return i;
        }
    }

    return Transaction("ERROR IN GETLOCKOWNER");
}