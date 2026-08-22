/*
 * streams.hpp
 * C++ streams API
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <servicelib/runtime/consumer.hpp>

namespace servicelib {
namespace detail {

template <typename K>
struct is_key_value : std::false_type {};

template <typename K, typename V>
struct is_key_value<KeyValueType<K, V>> : std::true_type {};

template <typename T>
struct key_value_args;

template <typename K, typename V>
struct key_value_args<KeyValueType<K, V>> {
  using key_type = K;
  using value_type = V;
};

// hash helpers
template <typename T, typename Ctx, typename = void>
struct is_custom_hashable : std::false_type {};

template <typename T, typename Ctx>
struct is_custom_hashable<
    T, Ctx,
    std::void_t<decltype(std::declval<typename Ctx::DataTypeFactory::template DataType<std::decay_t<T>>::hash_type>()(
        std::declval<const std::decay_t<T>&>()))>>
    : std::is_same<std::size_t, decltype(std::declval<typename Ctx::DataTypeFactory::template DataType<
                                             std::decay_t<T>>::hash_type>()(std::declval<const std::decay_t<T>&>()))> {
};

template <typename T, typename Ctx>
using is_hashable =
    std::disjunction<is_custom_hashable<std::decay_t<T>, Ctx>, detail::is_std_hashable<std::decay_t<T>>>;

template <typename T, typename Ctx, typename = void>
struct hash_type_helper {};

template <typename T, typename Ctx>
struct hash_type_helper<T, Ctx, std::enable_if_t<is_custom_hashable<std::decay_t<T>, Ctx>::value>> {
  using type = typename Ctx::DataTypeFactory::template DataType<std::decay_t<T>>::hash_type;
};

template <typename T, typename Ctx>
struct hash_type_helper<T, Ctx,
                        std::enable_if_t<detail::is_std_hashable<std::decay_t<T>>::value &&
                                         !is_custom_hashable<std::decay_t<T>, Ctx>::value>> {
  using type = std::hash<std::decay_t<T>>;
};

// equal helpers
template <typename T, typename Ctx, typename = void>
struct is_custom_equal_to : std::false_type {};

template <typename T, typename Ctx>
struct is_custom_equal_to<
    T, Ctx,
    std::void_t<decltype(std::declval<typename Ctx::DataTypeFactory::template DataType<
                             std::decay_t<T>>::equal_to_type>()(std::declval<const std::decay_t<T>&>()))>>
    : std::is_same<bool, decltype(std::declval<typename Ctx::DataTypeFactory::template DataType<
                                      std::decay_t<T>>::equal_to_type>()(std::declval<const std::decay_t<T>&>()))> {};

template <typename T, typename Ctx>
using is_equal_to =
    std::disjunction<is_custom_equal_to<std::decay_t<T>, Ctx>, detail::is_std_equal_to<std::decay_t<T>>>;

template <typename T, typename Ctx, typename = void>
struct equal_to_type_helper {};

template <typename T, typename Ctx>
struct equal_to_type_helper<T, Ctx, std::enable_if_t<is_custom_equal_to<std::decay_t<T>, Ctx>::value>> {
  using type = typename Ctx::DataTypeFactory::template DataType<std::decay_t<T>>::equal_to_type;
};

template <typename T, typename Ctx>
struct equal_to_type_helper<T, Ctx,
                            std::enable_if_t<detail::is_std_equal_to<std::decay_t<T>>::value &&
                                             !is_custom_equal_to<std::decay_t<T>, Ctx>::value>> {
  using type = std::equal_to<std::decay_t<T>>;
};

// serde helpers
template <typename T, typename Ctx, typename = void>
struct has_serialize_deserialize : std::false_type {};

template <typename T, typename Ctx>
struct has_serialize_deserialize<
    T, Ctx,
    std::void_t<decltype(std::declval<typename Ctx::DataTypeFactory::template DataType<std::decay_t<T>>::serde_type>()
                             .serialize(std::declval<serde_data&>(), std::declval<const std::decay_t<T>&>())),
                decltype(std::declval<typename Ctx::DataTypeFactory::template DataType<std::decay_t<T>>::serde_type>()
                             .deserialize(std::declval<serde_data::const_iterator&>(),
                                          std::declval<std::decay_t<T>&>())),
                decltype(std::decay_t<T>())>>
    : std::conjunction<
          std::is_same<
              decltype(std::declval<typename Ctx::DataTypeFactory::template DataType<std::decay_t<T>>::serde_type>()
                           .serialize(std::declval<serde_data&>(), std::declval<const std::decay_t<T>&>())),
              serde_data&>,
          std::is_same<
              decltype(std::declval<typename Ctx::DataTypeFactory::template DataType<std::decay_t<T>>::serde_type>()
                           .deserialize(std::declval<serde_data::const_iterator&>(), std::declval<std::decay_t<T>&>())),
              serde_data::const_iterator>> {};

template <typename T, typename Ctx, typename = void>
struct serde_type_helper {};

template <typename T, typename Ctx>
struct serde_type_helper<T, Ctx, std::enable_if_t<has_serialize_deserialize<std::decay_t<T>, Ctx>::value>> {
  using type = typename Ctx::DataTypeFactory::template DataType<std::decay_t<T>>::serde_type;
};

template <typename T, typename Ctx>
using is_serialization_necessary =
    std::negation<std::conjunction<std::is_copy_constructible<std::decay_t<T>>, is_equal_to<std::decay_t<T>, Ctx>,
                                   is_hashable<std::decay_t<T>, Ctx>>>;

template <typename T>
struct getter {
  T& operator()(T& v) const noexcept { return v; }
};

template <typename T, typename Serde>
struct serialized_getter {
  T operator()(serde_data::const_iterator iter) const {
    T v;
    Serde{}.deserialize(iter, v);
    return v;
  }
};

template <typename T>
struct setter {
  const T& operator()(const T& v) const noexcept { return v; }
};

template <typename T, typename Serde>
struct serialized_setter {
  serde_data operator()(const T& v) const {
    serde_data data;
    Serde{}.serialize(data, v);
    return data;
  }
};

template <typename K, typename Ctx, typename = void>
struct memory_storage_key_type_helper : std::false_type {};

template <typename K, typename Ctx>
struct memory_storage_key_type_helper<K, Ctx,
                                      std::enable_if_t<std::negation<is_serialization_necessary<K, Ctx>>::value>>
    : std::true_type {
  using ktype = std::decay_t<K>;
  using equal_to_type = typename equal_to_type_helper<std::decay_t<K>, Ctx>::type;
  using hash_type = typename hash_type_helper<std::decay_t<K>, Ctx>::type;
  using getter_type = getter<K>;
  using setter_type = setter<K>;
};

template <typename K, typename Ctx>
struct memory_storage_key_type_helper<
    K, Ctx, std::enable_if_t<std::conjunction_v<is_serialization_necessary<K, Ctx>, has_serialize_deserialize<K, Ctx>>>>
    : std::true_type {
  using ktype = serde_data;
  using equal_to_type = std::equal_to<serde_data>;
  using hash_type = std::hash<serde_data>;
  using getter_type = serialized_getter<K, typename serde_type_helper<K, Ctx>::type>;
  using setter_type = serialized_setter<K, typename serde_type_helper<K, Ctx>::type>;
};

template <typename V, typename Ctx, typename = void>
struct memory_storage_value_type_helper : std::false_type {};

template <typename V, typename Ctx>
struct memory_storage_value_type_helper<V, Ctx, std::enable_if_t<std::is_copy_constructible_v<std::decay_t<V>>>>
    : std::true_type {
  using vtype = std::decay_t<V>;
  using getter_type = getter<V>;
  using setter_type = setter<V>;
};

template <typename V, typename Ctx>
struct memory_storage_value_type_helper<
    V, Ctx,
    std::enable_if_t<std::conjunction_v<std::negation<std::is_copy_constructible<std::decay_t<V>>>,
                                        has_serialize_deserialize<V, Ctx>>>> : std::true_type {
  using vtype = serde_data;
  using getter_type = serialized_getter<V, typename serde_type_helper<V, Ctx>::type>;
  using setter_type = serialized_setter<V, typename serde_type_helper<V, Ctx>::type>;
};

template <typename T1, typename T2, typename Ctx, typename = void>
struct can_use_memory_join : std::false_type {};

template <typename T1, typename T2, typename Ctx>
struct can_use_memory_join<
    T1, T2, Ctx,
    std::enable_if_t<
        is_key_value<T1>::value && is_key_value<T2>::value &&
        detail::is_comparable<typename key_value_args<T1>::key_type, typename key_value_args<T2>::key_type>::value &&
        memory_storage_key_type_helper<typename key_value_args<T1>::key_type, Ctx>::value &&
        memory_storage_key_type_helper<typename key_value_args<T2>::key_type, Ctx>::value &&
        memory_storage_value_type_helper<typename key_value_args<T1>::value_type, Ctx>::value &&
        memory_storage_value_type_helper<typename key_value_args<T2>::value_type, Ctx>::value>> : std::true_type {};

template <typename T1, typename T2, typename Ctx, JoinStrategyEnum joinStrategy, typename = void>
struct can_join : std::false_type {};

template <typename T1, typename T2, typename Ctx>
struct can_join<T1, T2, Ctx, JoinStrategyEnum::InMemory, std::enable_if_t<can_use_memory_join<T1, T2, Ctx>::value>>
    : std::true_type {};

}  // namespace detail
}  // namespace servicelib
