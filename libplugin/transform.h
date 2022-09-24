#pragma once

#include<iostream>

using namespace std;

namespace dev 
{
    namespace plugin
    {
        class transform
        {
            public:
                unsigned int hex_char_to_dec(char c);

                unsigned int str_to_hex(const unsigned char *str);

                int strhex_parse_hex(std::string in,unsigned char *out);

                void hexstring_from_data(unsigned char *data, int len, char *output);

                // std::string hexstring_from_data(const void *data, size_t len);

                // std::string hexstring_from_data(const std::string &data);
        };
    }
}