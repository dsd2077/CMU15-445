#include <stdexcept>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "common/rwlatch.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() -> bool {
  root_page_id_latch_.RLock();
  if (root_page_id_ == INVALID_PAGE_ID) {
    root_page_id_latch_.RUnlock();
    return true;
  }
  root_page_id_latch_.RUnlock();
  return false;
  // Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  // root_page_id_latch_.RUnlock();
  // assert(page != nullptr);

  // auto bpt_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // int size = bpt_page->GetSize();
  // buffer_pool_manager_->UnpinPage(bpt_page->GetPageId(), false);
  // return size == 0;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
// TODO(me) : 为什么result用vector来接收？
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }

  root_page_id_latch_.RLock();
  Page *page = FindLeafOptimistically(key, OpType::SEARCH);
  assert(page != nullptr);

  auto bplus_leaf_page = reinterpret_cast<LeafPage *>(page);
  ValueType v;
  bool is_existed = bplus_leaf_page->Lookup(key, v, this);
  result->push_back(v);

  // You have to release the latch on that page before you unpin the same page from the buffer pool.
  page->RUnlatch();
  UnpinPage(bplus_leaf_page->GetPageId(), false);
  return is_existed;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // root_page_id_latch_伴随根节点的释放而释放
  root_page_id_latch_.RLock();
  if (root_page_id_ == INVALID_PAGE_ID) {
    root_page_id_latch_.RUnlock();
    root_page_id_latch_.WLock();
    if (root_page_id_ == INVALID_PAGE_ID) {  // 解锁之后，锁有可能被别人抢占，所以需要再次检测
      CreateANewRootPage(key, value);
      root_page_id_latch_.WUnlock();
      return true;
    }
    root_page_id_latch_.WUnlock();  // 这个写锁需要释放掉，因为后面要尝试加乐观锁
    root_page_id_latch_.RLock();    // 重新将读锁加上
  }

  // 先尝试加乐观锁
  // 乐观锁只需要对叶子节点加写锁，不需要transaction参数,FindLeafOptimistically内部一定已经对root_page_id_latch_解锁了
  Page *page = FindLeafOptimistically(key, OpType::INSERT);
  auto bpt_leaf_page = reinterpret_cast<LeafPage *>(page);
  ValueType temp;
  if (bpt_leaf_page->Lookup(key, temp, this)) {
    page->WUnlatch();
    UnpinPage(page->GetPageId(), true);
    return false;
  }

  if (bpt_leaf_page->GetSize() < bpt_leaf_page->GetMaxSize() - 1) {
    bpt_leaf_page->InsertOne(key, value, this);  // 确定节点安全直接调用insertOne
    page->WUnlatch();
    UnpinPage(page->GetPageId(), true);
    return true;
  }

  // 叶节点不安全，改为悲观锁,先将之前的锁释放
  page->WUnlatch();
  UnpinPage(page->GetPageId(), true);
  root_page_id_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);

  bpt_leaf_page = FindLeafPessimically(key, transaction, OpType::INSERT);
  bool res = bpt_leaf_page->Insert(key, value, this, transaction);
  ReleaseAllAncestorsLocks(transaction, true);
  return res;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CreateANewRootPage(const KeyType &key, const ValueType &value) {
  Page *new_page = buffer_pool_manager_->NewPage(&root_page_id_);
  assert(new_page != nullptr);

  auto *bpt_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  bpt_leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);

  bpt_leaf_page->InsertOne(key, value, this);

  UpdateRootPageId(1);
  UnpinPage(root_page_id_, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_page_id_latch_.RLock();
  if (root_page_id_ == INVALID_PAGE_ID) {
    root_page_id_latch_.RUnlock();
    return;
  }
  // 乐观锁
  Page *page = FindLeafOptimistically(key, OpType::DELETE);
  auto bpt_leaf_page = reinterpret_cast<LeafPage *>(page);
  if (bpt_leaf_page->GetSize() > bpt_leaf_page->GetMinSize()) {
    bpt_leaf_page->Remove(key, this, transaction);
    page->WUnlatch();
    UnpinPage(page->GetPageId(), true);
    return;
  }

  // 悲观锁
  page->WUnlatch();
  UnpinPage(page->GetPageId(), true);
  root_page_id_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);

  bpt_leaf_page = FindLeafPessimically(key, transaction, OpType::DELETE);
  bpt_leaf_page->Remove(key, this, transaction);

  ReleaseAllAncestorsLocks(transaction, true);

  for (auto &pid : *(transaction->GetDeletedPageSet())) {
    buffer_pool_manager_->DeletePage(pid);
  }
  transaction->GetDeletedPageSet()->clear();
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(this, nullptr, nullptr, 0);  // 返回无效迭代器
  }
  root_page_id_latch_.RLock();
  Page *parent_page = FindLeafOptimistically(KeyType(), OpType::SEARCH, true);
  auto leaf_page = reinterpret_cast<LeafPage *>(parent_page);
  return INDEXITERATOR_TYPE(this, parent_page, leaf_page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(this, nullptr, nullptr, 0);  // 返回无效迭代器
  }

  root_page_id_latch_.RLock();
  Page *page = FindLeafOptimistically(key, OpType::SEARCH);
  auto leaf_page = reinterpret_cast<LeafPage *>(page);
  ValueType value;
  // 如果key不存在
  if (!leaf_page->Lookup(key, value, this)) {
    page->RUnlatch();
    UnpinPage(page->GetPageId(), false);
    return INDEXITERATOR_TYPE(this, nullptr, nullptr, 0);  // 返回无效迭代器
  }
  int pos = leaf_page->LowerBound(key, this);
  return INDEXITERATOR_TYPE(this, page, leaf_page, pos);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(this, nullptr, nullptr, 0);  // 返回无效迭代器
  }

  root_page_id_latch_.RLock();
  Page *page = FindLeafOptimistically(KeyType(), OpType::SEARCH, false, true);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page);
  return INDEXITERATOR_TYPE(this, page, leaf_page, leaf_page->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

// 从根节点开始搜索，找到可能存在key值的叶子结点
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafOptimistically(const KeyType &key, OpType op_type, bool left_most, bool right_most)
    -> Page * {
  // 外部已经加了root_page_id_latch_.RLock();
  Page *parent_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *cur_page = reinterpret_cast<BPlusTreePage *>(parent_page->GetData());
  if (op_type == OpType::SEARCH || !cur_page->IsLeafPage()) {
    parent_page->RLatch();
  } else {
    parent_page->WLatch();  // 只有insert/remove时，并且为叶子结点才加写锁
  }
  root_page_id_latch_.RUnlock();

  while (!cur_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(cur_page);
    page_id_t next_page_id;
    if (right_most) {
      next_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    } else if (left_most) {
      next_page_id = internal_page->ValueAt(0);
    } else {
      next_page_id = internal_page->Lookup(key, this);
    }
    Page *child_page = buffer_pool_manager_->FetchPage(next_page_id);
    cur_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    // 孩子节点加锁
    if (op_type == OpType::SEARCH || !cur_page->IsLeafPage()) {
      child_page->RLatch();
    } else {
      child_page->WLatch();  // 只有insert/remove时，并且为叶子结点才加写锁
    }
    // 父节点解锁
    parent_page->RUnlatch();
    UnpinPage(parent_page->GetPageId(), false);
    parent_page = child_page;
  }
  return parent_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafForSearch(const KeyType &key, bool &root_latch) -> Page * {
  // 必须对root_page_id进行互斥访问，否则有可能其他线程对它进行修改
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  assert(root_page != nullptr);
  root_page->RLatch();  // 先锁根节点

  Page *buffer_page_cur = root_page;
  auto *bpt_page_cur = reinterpret_cast<BPlusTreePage *>(root_page->GetData());

  while (!bpt_page_cur->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(bpt_page_cur);
    auto next_page_id = internal_page->Lookup(key, this);
    Page *child_page = buffer_pool_manager_->FetchPage(next_page_id);
    assert(child_page != nullptr);

    // 先锁孩子节点，再解锁父节点
    child_page->RLatch();
    buffer_page_cur->RUnlatch();
    UnpinPage(bpt_page_cur->GetPageId(), false);
    if (buffer_page_cur == root_page) {
      root_page_id_latch_.RUnlock();
      root_latch = false;
    }

    bpt_page_cur = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    buffer_page_cur = child_page;
  }
  return buffer_page_cur;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPessimically(const KeyType &key, Transaction *transaction, OpType op_type) -> LeafPage * {
  // 必须对root_page_id进行互斥访问，否则有可能其他线程对它进行修改
  Page *buffer_page_parent = buffer_pool_manager_->FetchPage(root_page_id_);  // 缓存池中的page
  assert(buffer_page_parent != nullptr);
  auto *bpt_page_cur = reinterpret_cast<BPlusTreePage *>(buffer_page_parent->GetData());

  buffer_page_parent->WLatch();  // 先锁根节点
  if (IsPageSafe(bpt_page_cur, op_type)) {
    ReleaseAllAncestorsLocks(transaction, false);
  }
  transaction->AddIntoPageSet(buffer_page_parent);

  while (!bpt_page_cur->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(bpt_page_cur);
    auto next_page_id = internal_page->Lookup(key, this);
    assert(next_page_id > 0);

    Page *buffer_page_child = buffer_pool_manager_->FetchPage(next_page_id);
    assert(buffer_page_child != nullptr);
    auto *bpt_page_child = reinterpret_cast<BPlusTreePage *>(buffer_page_child->GetData());

    // 先锁孩子节点，再解锁父节点
    buffer_page_child->WLatch();
    // 孩子节点安全，可以释放所有的祖先节点
    if (IsPageSafe(bpt_page_child, op_type)) {
      ReleaseAllAncestorsLocks(transaction, false);
    }
    transaction->AddIntoPageSet(buffer_page_child);

    buffer_page_parent = buffer_page_child;
    bpt_page_cur = bpt_page_child;
  }
  return reinterpret_cast<LeafPage *>(buffer_page_parent);
}
// auto FindLeafForDelete(const KeyType &key) -> LeafPage *;

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseAllAncestorsLocks(Transaction *transaction, bool is_dirty) {
  if (transaction == nullptr) {
    return;
  }
  // std::shared_ptr<std::deque<Page *>> ancestors = transaction->GetPageSet();
  // std::cout << "thread :" << std::this_thread::get_id() << " release ancestors : " ;

  while (!transaction->GetPageSet()->empty()) {
    Page *temp = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();

    if (temp == nullptr) {
      root_page_id_latch_.WUnlock();
      // std::cout << "Thread " << std::this_thread::get_id() << " released the lock. before insert\n";
    } else {
      temp->WUnlatch();
      UnpinPage(temp->GetPageId(), is_dirty);
      // std::cout << temp->GetPageId() << " ";
    }
  }
  // std::cout << std::endl;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsPageSafe(BPlusTreePage *bpt_page, OpType op) -> bool {
  if (op == OpType::INSERT) {
    // 叶子结点，插满之后就分裂，至少剩余两个空位为安全
    if (bpt_page->IsLeafPage()) {
      return bpt_page->GetSize() < bpt_page->GetMaxSize() - 1;
    }
    // 中间结点插之前进行分裂,至少剩余一个空位为安全
    return bpt_page->GetSize() < bpt_page->GetMaxSize();
  }
  if (op == OpType::DELETE) {
    // 非叶子根节点，当删除最后一个key时将第一个孩子当做新的根结点,
    // 如果一个节点既是根节点也是叶结点，此时树中只有一个节点，无论如何都不会进行分裂
    if (bpt_page->IsRootPage()) {
      if (bpt_page->IsLeafPage()) {
        return bpt_page->GetSize() > 1;
      }
      return bpt_page->GetSize() > 2;
    }
    return bpt_page->GetSize() > bpt_page->GetMinSize();
  }
  return false;
}
/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
// Since any write operation could lead to the change of root_page_id in B+Tree index,
// it is your responsibility to update root_page_id in the header page
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  header_page->WLatch();
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  header_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(page_id_t new_root_page_id) {
  int flag = static_cast<int>(root_page_id_ == INVALID_PAGE_ID);
  root_page_id_ = new_root_page_id;
  UpdateRootPageId(flag);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CompareKey(const KeyType &lkey, const KeyType &rkey) -> int { return comparator_(lkey, rkey); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewPage(page_id_t *page_id) -> Page * { return buffer_pool_manager_->NewPage(page_id); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchPage(page_id_t page_id) -> Page * { return buffer_pool_manager_->FetchPage(page_id); }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnpinPage(page_id_t page_id, bool is_dirty) { buffer_pool_manager_->UnpinPage(page_id, is_dirty); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeletePage(page_id_t page_id) -> bool { return buffer_pool_manager_->DeletePage(page_id); }

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
