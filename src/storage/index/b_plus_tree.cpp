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
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  root_page_id_latch_.RUnlock();
  assert(page != nullptr);

  auto bpt_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  int size = bpt_page->GetSize();
  buffer_pool_manager_->UnpinPage(bpt_page->GetPageId(), false);
  return size == 0;
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
  Page *page = FindLeafForSearch(key);
  assert(page != nullptr);
  auto bplus_leaf_page = reinterpret_cast<LeafPage *>(page);
  ValueType v;
  bool is_existed = bplus_leaf_page->Lookup(key, v, this);
  if (!is_existed) {
    return false;
  }
  result->push_back(v);

  // You have to release the latch on that page before you unpin the same page from the buffer pool.
  page->RUnlatch();
  UnpinPage(bplus_leaf_page->GetPageId(), false);
  return true;
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
  if (root_page_id_ == INVALID_PAGE_ID) {
    page_id_t page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&page_id);
    assert(new_page != nullptr);

    auto *bpt_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
    bpt_leaf_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);  // 为什么这里无法完成跳转？
    SetRootPageId(page_id);
    UnpinPage(page_id, true);
  }
  LeafPage *bplus_leaf_node = FindLeaf(key);
  bool res = bplus_leaf_node->Insert(key, value, this);
  UnpinPage(bplus_leaf_node->GetPageId(), true);
  return res;
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
  if (IsEmpty()) {
    return;
  }
  LeafPage *leaf_page = FindLeaf(key);
  leaf_page->Remove(key, this);
  UnpinPage(leaf_page->GetPageId(), true);
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
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(this, nullptr, 0);  // 返回无效迭代器
  }
  // 找到最左叶结点
  LeafPage *leaf_page = FindLeftMostLeafPage();
  return INDEXITERATOR_TYPE(this, leaf_page, 0);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftMostLeafPage() -> LeafPage * {
  auto *cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!cur_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(cur_page);
    auto next_page_id = internal_page->ValueAt(0);
    UnpinPage(cur_page->GetPageId(), false);
    cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
  }
  return reinterpret_cast<LeafPage *>(cur_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindRightMostLeafPage() -> LeafPage * {
  auto *cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!cur_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(cur_page);
    auto next_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    UnpinPage(cur_page->GetPageId(), false);
    cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
  }
  return reinterpret_cast<LeafPage *>(cur_page);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(this, nullptr, 0);  // 返回无效迭代器
  }
  LeafPage *leaf_page = FindLeaf(key);
  ValueType value;
  // 如果key不存在直接抛出错误？
  if (!leaf_page->Lookup(key, value, this)) {
    throw std::runtime_error("Begin(const KeyType &key) error : cann't find the key");
  }
  int pos = leaf_page->LowerBound(key, this);
  return INDEXITERATOR_TYPE(this, leaf_page, pos);  // 难道是因为没有赋值运算符，所以无法返回临时对象？
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(this, nullptr, 0);  // 返回无效迭代器
  }

  LeafPage *leaf_page = FindRightMostLeafPage();
  return INDEXITERATOR_TYPE(this, leaf_page, leaf_page->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

// 从根节点开始搜索，找到可能存在key值的叶子结点
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key) -> LeafPage * {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return nullptr;
  }
  auto *cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (!cur_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(cur_page);
    auto next_page_id = internal_page->Lookup(key, this);
    UnpinPage(cur_page->GetPageId(), false);
    cur_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
  }
  return reinterpret_cast<LeafPage *>(cur_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafForSearch(const KeyType &key) -> Page * {
  root_page_id_latch_.RLock();
  Page *parent_page = buffer_pool_manager_->FetchPage(
      root_page_id_);  // 有BUG：必须对root_page_id进行互斥访问，否则有可能其他线程对它进行修改
  root_page_id_latch_.RUnlock();

  parent_page->RLatch();  // 先锁根节点
  auto *cur_page = reinterpret_cast<BPlusTreePage *>(parent_page->GetData());

  while (!cur_page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(cur_page);
    auto next_page_id = internal_page->Lookup(key, this);  // 这里为什么会返回0？
    Page *child_page = buffer_pool_manager_->FetchPage(next_page_id);

    // 先锁孩子节点，再解锁父节点
    child_page->RLatch();
    parent_page->RUnlatch();
    UnpinPage(cur_page->GetPageId(), false);

    cur_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    parent_page = child_page;
  }
  return parent_page;
}
// auto FindLeafForInsert(const KeyType &key) -> LeafPage *;
// auto FindLeafForDelete(const KeyType &key) -> LeafPage *;

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
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
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
  int flag = static_cast<int>(root_page_id_ == 0);
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
