#include "transform.h"

using namespace dev::plugin;
using namespace std;

unsigned int transform::hex_char_to_dec(char c)
{
    if ('0' <= c && c <= '9')
    {
        return (c - '0');
    }
    else if ('a' <= c && c <= 'f')
    {
        return (c - 'a' + 10);
    }
    else if ('A' <= c && c <= 'F')
    {
        return (c - 'A' + 10);
    }
    else
    {
        return -1;
    }
}

unsigned int transform::str_to_hex(const unsigned char *str)
{
    return (str[1] == '\0') ? hex_char_to_dec(str[0]) : hex_char_to_dec(str[0])*16 + hex_char_to_dec(str[1]);
}

int transform::strhex_parse_hex(std::string in,unsigned char *out)
{
    unsigned char tmp[2] = { 0 };
    unsigned int len = in.size() / 2;
    for (int i = 0; i < len; i++)
    {
        int idx = i * 2;
        tmp[0] = in.at(idx);
        tmp[1] = in.at(idx+1);
        out[i] = (unsigned char)str_to_hex(tmp);
    }
    return len;
}


void transform::hexstring_from_data(unsigned char *data, int len, char *output) {
    unsigned char *buf = data;
    size_t i, j;
    for (i = j = 0; i < len; ++i) {
        char c;
        c = (buf[i] >> 4) & 0xf;
        c = (c > 9) ? c + 'a' - 10 : c + '0';
        output[j++] = c;
        c = (buf[i] & 0xf);
        c = (c > 9) ? c + 'a' - 10 : c + '0';
        output[j++] = c;
    }
}

// std::string transform::hexstring_from_data(const void *data, size_t len) {
//     if (len == 0) {
//         return std::string();
//     }

//     std::cout << "len = " << len << std::endl;
//     std::string result;
//     result.resize(len * 2);
//     hexstring_from_data(data, len, &result[0]);
//     return result;
// }

// std::string transform::hexstring_from_data(const std::string &data) {
//     return hexstring_from_data(data.c_str(), data.size());
// }