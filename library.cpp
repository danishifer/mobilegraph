#include "library.h"

#include <iostream>
#include <fstream>
#include <list>
#include <unordered_map>

using namespace std;

#define NODE_IN_USE_SIZE 1
#define NODE_IN_USE_OFFSET 0
#define NODE_EXTERNAL_ID_SIZE 4
#define NODE_EXTERNAL_ID_OFFSET (NODE_IN_USE_OFFSET + NODE_IN_USE_SIZE)
#define NODE_FIRST_PROP_SIZE 4
#define NODE_FIRST_PROP_OFFSET (NODE_EXTERNAL_ID_OFFSET + NODE_EXTERNAL_ID_SIZE)
#define NODE_REL_SIZE 4
#define NODE_REL_OFFSET (NODE_FIRST_PROP_OFFSET + NODE_FIRST_PROP_SIZE)
#define NODE_LABELS_SIZE 6
#define NODE_LABELS_OFFSET (NODE_REL_OFFSET + NODE_REL_SIZE)
#define NODE_MORE_LABELS_SIZE 1
#define NODE_MORE_LABELS_OFFSET (NODE_LABELS_OFFSET + NODE_LABELS_SIZE)
#define NODE_SIZE (NODE_MORE_LABELS_OFFSET + NODE_MORE_LABELS_SIZE)

class Node {
private:
    inline void get_label(uint8_t pos, uint8_t &page_num, uint8_t &slot_num) {
        memcpy(&page_num, bytes + NODE_LABELS_OFFSET + pos * 2, 1);
        memcpy(&slot_num, bytes + NODE_LABELS_OFFSET + pos * 2 + 1, 1);
    }

    inline void set_label(uint8_t pos, const uint8_t &page_num, const uint8_t &slot_num) {
        memcpy(bytes + NODE_LABELS_OFFSET + pos * 2, &page_num, 1);
        memcpy(bytes + NODE_LABELS_OFFSET + pos * 2 + 1, &slot_num, 1);
    }

public:
    unsigned char bytes[NODE_SIZE]{
            0x01,                   // in use
            0x00, 0x00, 0x00, 0x00, // global id
            0x00, 0x00, 0x00, 0x00, // first prop
            0x00, 0x00, 0x00, 0x00, // first rel
            0xFF, 0x00,             // first label
            0xFF, 0x00,             // second label
            0xFF, 0x00,             // third label
            0x00,                   // additional labels
    };

    Node() = default;

    explicit Node(char *ptr) {
        memcpy(bytes, ptr, NODE_SIZE);
    }

    inline void set_in_use(bool in_use) {
        memset(bytes + NODE_IN_USE_OFFSET, in_use, NODE_IN_USE_SIZE);
    }

    inline void set_external_id(uint32_t id) {
        memcpy(bytes + NODE_EXTERNAL_ID_OFFSET, &id, NODE_EXTERNAL_ID_SIZE);
    }

    void add_label(uint8_t page_num, uint8_t slot_num) {
        uint8_t p, s;
        for (int i = 0; i < 3; ++i) {
            get_label(i, p, s);
            if (p == 0xFF) {
                // label is empty
                set_label(i, page_num, slot_num);
                return;
            }

            if (p == page_num && s == slot_num) {
                return;
            }
        }

        // TODO: label overflow
        cerr << "Need to implement label overflow.\n";
        exit(EXIT_FAILURE);
    }

    void get_labels(uint8_t (&a)[3][2]) {
        for (int i = 0; i < 3; ++i) {
            get_label(i, a[i][0], a[i][1]);
        }
    }
};

class FileHandler {
    fstream file;
public:
    streampos file_length;

    explicit FileHandler(const std::string &filename) {
        file.open(filename.data(), ios::in | ios::out | ios::binary);
        if (!file.is_open()) {
            file.open(filename.data(), ios::in | ios::out | ios::trunc | ios::binary);
        }

        file.seekg(0, ios::end);
        file_length = file.tellg();
        file.seekg(0);
    }

    ~FileHandler() {
        file.close();
    }

    bool write(off_t offset, const char *data, streamsize len) {
        file.seekp(offset, ios::beg);
        file.write(data, len);
        return true;
    }

    bool read(char *into, off_t offset, streamsize len) {
        file.seekg(offset, ios::beg);
        file.read(into, len);
        file.clear(); // remove eof flag
        return true;
    }
};

#define PAGE_LENGTH_SIZE 2
#define PAGE_LENGTH_OFFSET 0
#define PAGE_DATA_SIZE 4094
#define PAGE_DATA_OFFSET (PAGE_LENGTH_OFFSET + PAGE_LENGTH_SIZE)
#define PAGE_SIZE (PAGE_DATA_OFFSET + PAGE_DATA_SIZE)

class Page {

public:
    uint32_t page_number;
    char bytes[PAGE_SIZE]{};

    explicit Page(uint32_t page_number) : page_number(page_number) {}

    inline uint16_t *data_length() {
        return reinterpret_cast<uint16_t *>(bytes + PAGE_LENGTH_OFFSET);
    }

    char *data_at_offset(unsigned int offset) {
        return bytes + PAGE_DATA_OFFSET + offset;
    }

    bool append(const char *data, unsigned int len, uint8_t &raw_offset) {
        bool rc = false;

        if (*data_length() + len >= PAGE_DATA_SIZE) {
            return rc;
        }

        raw_offset = *data_length();
        rc = set_at_offset(data, raw_offset, len);

        *data_length() += len;

        return rc;
    }

    bool set_at_offset(const void *data, unsigned int offset, unsigned int len) {
        if (offset + len >= PAGE_SIZE) {
            return false;
        }

        memcpy(data_at_offset(offset), data, len);
        if (offset + len > *data_length()) {
            *data_length() = offset + len;
        }

        return true;
    }
};

class Pager {
    bool manages_file_handler = false;
    FileHandler *file_handler;

private:
    Page *load_from_disk(uint32_t key) {
        Page *page = new Page(key);
        file_handler->read(page->bytes, key * PAGE_SIZE, PAGE_SIZE);
        return page;
    }

    Page *load_page(uint32_t key) {
        // evict least frequently used page from cache
        if (cache_holder.size() == cache_cap) {
            auto key_to_evict_it = cache_ref_list.front().second.begin();
            delete cache_holder[*key_to_evict_it].page;
            cache_holder.erase(*key_to_evict_it);
            cache_ref_list.front().second.erase(key_to_evict_it);

            if (cache_ref_list.front().second.empty())
                cache_ref_list.erase(cache_ref_list.begin());
        }

        // insert
        if (cache_ref_list.empty() || cache_ref_list.front().first != 1) {
            cache_ref_list.push_front({1, {key}});
        } else {
            cache_ref_list.front().second.push_back(key);
        }

        Page *new_page = load_from_disk(key);
        cache_holder[key] = {
                .page = new_page,
                .it_pair = cache_ref_list.begin(),
                .it_key = std::prev(cache_ref_list.front().second.end()),
        };
        return new_page;
    }

public:
    struct page_holder {
        Page *page;
        list<pair<int, list<uint32_t>>>::iterator it_pair;
        list<uint32_t>::iterator it_key;
    };

    Pager(const std::string &filename, int cache_capacity) : cache_cap(cache_capacity) {
        file_handler = new FileHandler(filename);
        manages_file_handler = true;
    }

    Pager(FileHandler *file_handler, int cache_capacity) : file_handler(file_handler), cache_cap(cache_capacity) {}

    ~Pager() {
        for (auto const&[key, val] : cache_holder) {
            delete val.page;
        }

        if (manages_file_handler) {
            delete file_handler;
        }
    }

    Page *get_page(uint32_t key) {
        auto it = cache_holder.find(key);

        if (it == cache_holder.end()) {
            // cache miss
            return load_page(key);
        } else {
            visit(it, key);
            return it->second.page;
        }
    }

    int last_page() {
        if (!file_handler->file_length) { return 0; }
        return static_cast<int>(file_handler->file_length / PAGE_SIZE) - 1;
    }

    void flush_page(Page *page) {
        file_handler->write(page->page_number * PAGE_SIZE, page->bytes, PAGE_SIZE);
    }

protected:
    void visit(typename unordered_map<uint32_t, page_holder>::iterator it, uint32_t key) {
        auto it_pair = it->second.it_pair;
        auto it_key = it->second.it_key;
        int count = it_pair->first + 1;

        it_pair->second.erase(it_key);
        if (it_pair->second.empty()) {
            it_pair = cache_ref_list.erase(it_pair);
        } else {
            std::advance(it_pair, 1);
        }

        if (it_pair == cache_ref_list.end() || it_pair->first != count) {
            it_pair = cache_ref_list.insert(it_pair, {count, {key}});
        } else {
            it_pair->second.push_back(key);
        }

        it->second.it_pair = it_pair;
        it->second.it_key = std::prev(it_pair->second.end());
    }

    int cache_cap;
    list<pair<int, list<uint32_t>>> cache_ref_list;
    unordered_map<uint32_t, page_holder> cache_holder;
};

class NodeStore {
    Pager *pager;
public:
    explicit NodeStore(Pager *pager) : pager(pager) {}

    uint32_t insert(const Node &node) {
        int last_page = pager->last_page();
        Page *page = pager->get_page(last_page);

        uint16_t offset = *page->data_length();

        if (!page->set_at_offset(node.bytes, offset, NODE_SIZE)) {
            printf("Couldn't append to page %d\n", last_page);
        }

        pager->flush_page(page);

        return offset / NODE_SIZE;
    }

    Node *get(uint32_t id) {
        unsigned int page_num = id / PAGE_DATA_SIZE;
        Page *page = pager->get_page(page_num);
        return reinterpret_cast<Node *>(page->data_at_offset((id % PAGE_DATA_SIZE) * NODE_SIZE));
    }
};

#define LABEL_TEXT_SIZE 17
#define LABEL_TEXT_OFFSET 0
#define LABEL_NUM_PAGES_SIZE 1
#define LABEL_NUM_PAGES_OFFSET (LABEL_TEXT_OFFSET + LABEL_TEXT_SIZE)
#define LABEL_SIZE (LABEL_NUM_PAGES_OFFSET + LABEL_NUM_PAGES_SIZE)

class NodeLabel {
public:
    char *bytes;
//    char bytes[LABEL_SIZE]{};

    explicit NodeLabel(char *data) {
        bytes = data;
    }

    [[nodiscard]] string get_text() const {
        return string(bytes + LABEL_TEXT_OFFSET, LABEL_TEXT_SIZE);
    }

    inline bool is_text(const char *text) const {
        return std::strncmp(bytes + LABEL_TEXT_OFFSET, text, LABEL_TEXT_SIZE) == 0;
    }

    inline void set_text(const char *text) const {
        strncpy(bytes + LABEL_TEXT_OFFSET, text, LABEL_TEXT_SIZE);
    }

    [[nodiscard]] inline uint8_t *num_pages() const {
        return reinterpret_cast<uint8_t *>(bytes + LABEL_NUM_PAGES_OFFSET);
    }
};

// closest prime number to (PAGE_DATA_SIZE / LABEL_SIZE) (4094 / 18)
#define NODE_LABEL_STORE_TABLE_SIZE 227

class NodeLabelStore {
    Pager &pager;
private:
    uint8_t slot_for(const char *str) {
        // FIXME: Only for testing
        if (strcmp(str, "TEACHES_HOMEROOM") == 0) {
            return slot_for("TEACHES");
        } else if (strcmp(str, "COORDINATES") == 0) {
            return slot_for("TEACHES");
        }

        u_int32_t hash = 5381;

        for (uint8_t i = 0; i < LABEL_TEXT_SIZE; i++) {
            hash = ((hash << 5u) + hash) + str[i];
        }

        return hash % NODE_LABEL_STORE_TABLE_SIZE;
    }

public:
    explicit NodeLabelStore(Pager &pager) : pager(pager) {}

    void insert(const char *text, uint8_t &page_number, uint8_t &slot_number) {
        slot_number = slot_for(text);
        unsigned int page_offset = slot_number * LABEL_SIZE;

        page_number = 0;

        Page *first_page = pager.get_page(page_number);
        NodeLabel first_page_label(first_page->data_at_offset(page_offset));

        if (*first_page_label.num_pages() == 0) {
            // slot in first page is empty
            first_page_label.set_text(text);
            (*first_page_label.num_pages())++;

            first_page->set_at_offset(first_page_label.bytes, page_offset, LABEL_SIZE);
        } else if (first_page_label.is_text(text)) {
            return;
        } else {
            for (page_number = 1; page_number < *first_page_label.num_pages(); ++page_number) {
                Page *page = pager.get_page(page_number);
                NodeLabel page_label(page->data_at_offset(page_offset));
                if (page_label.is_text(text)) {
                    // label already exists
                    return;
                }
            }

            page_number = *first_page_label.num_pages();
            Page *page = pager.get_page(page_number);

            NodeLabel label(page->data_at_offset(page_offset));
            label.set_text(text);

            *first_page_label.num_pages() = page_number + 1;

            pager.flush_page(page);
        }

        pager.flush_page(first_page);
    }

    NodeLabel get(uint8_t page_number, uint8_t slot_number) {
        Page *page = pager.get_page(page_number);
        return NodeLabel(page->data_at_offset(slot_number * LABEL_SIZE));
    }
};

#define NODE_PROP_IN_USE_SIZE 1
#define NODE_PROP_IN_USE_OFFSET 0
#define NODE_PROP_KEY_SIZE 4
#define NODE_PROP_KEY_OFFSET (NODE_PROP_IN_USE_OFFSET + NODE_PROP_IN_USE_SIZE)
#define NODE_PROP_VALUE_SIZE 4
#define NODE_PROP_VALUE_OFFSET (NODE_PROP_KEY_OFFSET + NODE_PROP_KEY_SIZE)
#define NODE_PROP_NEXT_PROP_SIZE 4
#define NODE_PROP_NEXT_PROP_OFFSET (NODE_PROP_VALUE_OFFSET + NODE_PROP_VALUE_SIZE)
#define NODE_PROP_SIZE (NODE_PROP_NEXT_PROP_OFFSET + NODE_PROP_NEXT_PROP_SIZE)

class NodeProp {
    bool manages_memory = false;
public:
    unsigned char *bytes;

    NodeProp() {
        bytes = new unsigned char[NODE_PROP_SIZE]{
                0x01,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
        };

        manages_memory = true;
    }

    explicit NodeProp(unsigned char *data) {
        bytes = data;
    }

    ~NodeProp() {
        if (manages_memory) {
            delete[] bytes;
        }
    }
};

class NodePropStore {
    Pager &pager;
    Pager &key_pager;
    Pager &value_pager;
public:
    NodePropStore(Pager &pager, Pager &key_pager, Pager &value_pager) : pager(pager), key_pager(key_pager),
                                                                        value_pager(value_pager) {}

    void insert(const NodeProp &node_prop) {
        Page *page = pager.get_page(0);

        uint16_t offset = *page->data_length();

        page->set_at_offset(node_prop.bytes, offset, NODE_PROP_SIZE);

        pager.flush_page(page);
    }
};


void hello() {
    FileHandler nodes_file("mobilegraph.nodestore");
    Pager nodes_pager(&nodes_file, 2);
    NodeStore node_store(&nodes_pager);

    FileHandler node_props_file("mobilegraph.nodestore.props");
    Pager node_props_pager(&node_props_file, 2);

    Pager node_prop_key_pager("mobilegraph.nodestore.props.keys", 2);
    Pager node_prop_value_pager("mobilegraph.nodestore.props.vals", 2);
    NodePropStore node_prop_store(node_props_pager, node_prop_key_pager, node_prop_value_pager);

    NodeProp myprop;
    node_prop_store.insert(myprop);

    FileHandler node_labels_file("mobilegraph.nodestore.labels");
    Pager node_labels_pager(&node_labels_file, 2);
    NodeLabelStore node_label_store(node_labels_pager);


    Node node;
    node.set_in_use(true);
    node.set_external_id(120);

    uint8_t page_num, slot_num;
    node_label_store.insert("TEACHES_HOMEROOM", page_num, slot_num);
    printf("Inserted at %d:%d\n", page_num, slot_num);
    node.add_label(page_num, slot_num);
    node_label_store.insert("COORDINATES", page_num, slot_num);
    node.add_label(page_num, slot_num);

    uint32_t id = 0;
    cout << "Inserted id: " << (id = node_store.insert(node)) << endl;

//    node_label_store.insert("TEACHES", page_num, slot_num);
//    printf("Inserted at %d:%d\n", page_num, slot_num);

    Node *my_node = node_store.get(id);

    uint8_t labels[3][2];
    my_node->get_labels(labels);
    for (uint8_t *i : labels) {
        if (i[0] != 0xFF) {
            cout << node_label_store.get(i[0], i[1]).get_text() << endl;
        }
    }


//    NodeLabel label;
//    label.set_text("TEACHES_HOMEROOM");

//    Node node;
//    node.set_in_use(true);
//    node.set_external_id(478652);
//    node.add_label(page_num, slot_num);
//    node_store.insert(node);

//    Page * page = nodes_pager.get_page(0);
//    for (char byte : page->bytes) {
//        printf("%02x", byte);
//    }



//    FileHandler props_file ("mobilegraph.nodestore.props");

//    nodes_file.add_node(54353);

    std::cout << "Hello, World!" << std::endl;
}
