#pragma once
#include <cstdint>
#include <climits>

namespace valuet{

    struct NewString{ // must be 64-bit (8 bytes)
        
        uint8_t table_id; // which table the string is in
        uint8_t column_id; // which column of the table the string is in
        uint32_t page_id; // which page of the column the string is in
        uint16_t offset_idx; // which offset of the page the string ends at (the previous offset is where it begins)
 
        NewString() = default;
        NewString(uint8_t table_id, uint8_t column_id, uint32_t page_id, uint16_t offset_idx) : table_id(table_id), column_id(column_id), page_id(page_id), offset_idx(offset_idx){}
    };

    union value_t{
        int32_t intvalue;
        NewString stringvalue;

        value_t() : intvalue(0){} // default will assign 0 to an int32_t
        value_t(int32_t value) : intvalue(value){}
        value_t(NewString value) : stringvalue(value){}

        // null check
        bool is_null_int32() const{
            return intvalue == INT32_MIN;
        }

        bool is_null_string() const{
            return stringvalue.table_id == 0xFF && stringvalue.column_id == 0xFF && stringvalue.page_id == 0xFFFFFFFF && stringvalue.offset_idx == 0xFFFF;
        }

        static value_t null_int32(){
            return value_t(INT32_MIN);
        }
        static value_t null_string(){
            return value_t(NewString(0xFF, 0xFF, 0xFFFFFFFF, 0xFFFF));
        }
    };

}