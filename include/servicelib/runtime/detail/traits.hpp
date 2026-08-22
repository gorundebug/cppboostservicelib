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

namespace servicelib {
namespace detail {

template <typename T, typename U, typename = void>
struct is_comparable : std::false_type {};

template <typename T, typename U>
struct is_comparable<T, U, decltype((std::declval<T>() == std::declval<U>()), void())> : std::true_type {};

template <typename T, typename = void>
struct is_iterable : std::false_type {};

template <typename T>
struct is_iterable<T, std::void_t<decltype(std::begin(std::declval<T>())), decltype(std::end(std::declval<T>()))>>
    : std::true_type {};

template <typename T>
constexpr bool is_iterable_v = is_iterable<T>::value;

template <typename T, typename Tuple>
struct is_type_in_tuple;

template <typename T, typename... Ts>
struct is_type_in_tuple<T, std::tuple<Ts...>> : std::disjunction<std::is_same<T, Ts>...> {};

template <typename T, typename... Ts>
struct type_index;

template <typename T, typename... Ts>
struct type_index<T, T, Ts...> : std::integral_constant<std::size_t, 0> {};

template <typename T, typename U, typename... Ts>
struct type_index<T, U, Ts...> : std::integral_constant<std::size_t, 1 + type_index<T, Ts...>::value> {};

template <typename T, typename Tuple>
struct index_of_type;

template <typename T, typename... Ts>
struct index_of_type<T, std::tuple<Ts...>> : type_index<T, Ts...> {};

template <size_t size, typename T, typename... Ts>
struct type_index_no_error : std::integral_constant<std::size_t, size> {};

template <size_t size, typename T, typename... Ts>
struct type_index_no_error<size, T, T, Ts...> : std::integral_constant<std::size_t, 0> {};

template <size_t size, typename T, typename U, typename... Ts>
struct type_index_no_error<size, T, U, Ts...>
    : std::integral_constant<std::size_t, 1 + type_index_no_error<size, T, Ts...>::value> {};

template <typename T, typename Tuple>
struct index_of_type_no_error;

template <typename T, typename... Ts>
struct index_of_type_no_error<T, std::tuple<Ts...>> : type_index_no_error<sizeof...(Ts), T, Ts...> {};

// remove type from variant
template <template <typename> typename PtrType, typename TupleType,
          typename Indices = std::make_index_sequence<std::tuple_size_v<TupleType>>>
struct tuple_to_variant_helper;

template <template <typename> typename PtrType, typename... Types, std::size_t... Is>
struct tuple_to_variant_helper<PtrType, std::tuple<Types...>, std::index_sequence<Is...>> {
  static constexpr std::size_t indices[sizeof...(Types)] = {
      type_index<std::remove_reference_t<Types>, std::remove_reference_t<Types>...>::value...};
  using type =
      std::variant<std::conditional_t<(Is == indices[Is]), PtrType<std::remove_reference_t<Types>>, std::monostate>...>;
};

template <typename Type, typename... Types>
struct remove_type_from_variant;

template <typename Type, typename... Types>
struct remove_type_from_variant<Type, std::variant<Types...>> {
  using filtered_types =
      decltype(std::tuple_cat(std::conditional_t<std::is_same_v<Types, Type>, std::tuple<>, std::tuple<Types>>{}...));

  template <typename Tuple, std::size_t... Is>
  static auto make_variant_helper(Tuple&& t, std::index_sequence<Is...>)
      -> std::variant<std::tuple_element_t<Is, std::remove_reference_t<Tuple>>...>;

  using type =
      decltype(make_variant_helper(filtered_types{}, std::make_index_sequence<std::tuple_size_v<filtered_types>>{}));
};

template <typename Type, typename Variant>
using remove_type_from_variant_t = typename remove_type_from_variant<Type, Variant>::type;

// std::variant type for any
// std::remove_reference_t<std::tuple_element_t<std::tuple>>
template <typename TupleType, template <typename> typename PtrType>
using tuple_to_variant_uptr =
    remove_type_from_variant_t<std::monostate, typename tuple_to_variant_helper<PtrType, TupleType>::type>;

template <template <typename> typename PtrType, typename... Types>
struct variant_to_tuple_helper;

template <template <typename> typename PtrType, typename... Types>
struct variant_to_tuple_helper<PtrType, std::variant<Types...>> {
  using type = std::tuple<typename Types::element_type...>;
};

// make type
// std::variant<std::unique_ptr<std::remove_reference_t<std::tuple_element_t<I...>>>
// for tuple type
template <typename Variant, template <typename> typename PtrType>
using variant_to_tuple = typename variant_to_tuple_helper<PtrType, Variant>::type;

template <typename TupleType, template <typename> typename PtrType>
using tuple_to_unique_tuple = variant_to_tuple<tuple_to_variant_uptr<TupleType, PtrType>, PtrType>;

template <typename T, typename V, size_t... Is>
constexpr std::array<V, sizeof...(Is)> make_array_variant(std::index_sequence<Is...>) {
  return {(static_cast<void>(Is), V(T::make()))...};
}

template <typename V, typename Types>
struct make_array_variant_all;

template <typename V, typename... Types>
struct make_array_variant_all<V, std::tuple<Types...>> {
  static constexpr std::array<V, sizeof...(Types)> make_array() {
    return std::array<V, sizeof...(Types)>{V(Types::make())...};
  }
};

template <size_t I, typename T>
struct tuple_n {
  template <typename... Args>
  using type = typename tuple_n<I - 1, T>::template type<T, Args...>;
};

template <typename T>
struct tuple_n<0, T> {
  template <typename... Args>
  using type = std::tuple<Args...>;
};

template <size_t I, typename T>
using tuple_of = typename tuple_n<I, T>::template type<>;

template <template <typename> typename PtrType, typename Tuple, typename Array, std::size_t... Is>
constexpr auto make_tuple_helper(const Array& arr, std::index_sequence<Is...>) {
  return std::apply(
      [](auto&&... args) constexpr {
        return std::make_tuple(
            std::ref(*std::get<PtrType<std::remove_reference_t<typename std::tuple_element<Is, Tuple>::type>>>(
                std::forward<decltype(args)>(args)))...);
      },
      arr);
}

template <template <typename> typename PtrType, typename Tuple, std::size_t N, typename... Ts>
constexpr auto make_tuple(const std::array<std::variant<PtrType<Ts>...>, N>& arr) {
  return make_tuple_helper<PtrType, Tuple>(arr, std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}

template <typename T, typename = void>
struct is_std_hashable : std::false_type {};

template <typename T>
struct is_std_hashable<T, std::void_t<decltype(std::hash<T>{}(std::declval<T>()))>> : std::true_type {};

template <typename T, typename = void>
struct is_std_equal_to : std::false_type {};

template <typename T>
struct is_std_equal_to<T, std::void_t<decltype(std::equal_to<T>{}(std::declval<T>(), std::declval<T>()))>>
    : std::true_type {};

template <typename T, template <typename...> typename Ref>
struct is_specialization : std::false_type {};

template <template <typename...> typename Ref, typename... Args>
struct is_specialization<Ref<Args...>, Ref> : std::true_type {};

}  // namespace detail
}  // namespace servicelib
