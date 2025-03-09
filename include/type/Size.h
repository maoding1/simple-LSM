#pragma once

#include <iostream>
#include <string>
#include <type_traits>
// for integral types use sizeof()
template <typename T>
typename std::enable_if<std::is_integral<T>::value, size_t>::type
GetSize(const T& value) {
    return sizeof(value);
}

// for string use size()
template <typename T>
typename std::enable_if<std::is_same<T, std::string>::value, size_t>::type
GetSize(const T& value) {
    return value.size();
}