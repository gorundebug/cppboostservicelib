/*
 * streams.hpp
 * C++ streams API
 *
 * Copyright (c) 2024 Sergey Alexeev
 * Email: sergeyalexeev@yahoo.com
 *
 *  Licensed under the MIT License. See the
 * [LICENSE](https://opensource.org/licenses/MIT) file for details.
 */
#pragma once

#include <cstdint>
#include <algorithm>
#include <type_traits>
#include <vector>

#include <servicelib/runtime/base.hpp>
#include <servicelib/runtime/consumer.hpp>

namespace servicelib {

struct TopologyNode {
  size_t id;
  size_t configId;
  std::string name;
  std::string typeName;
  std::int64_t emittedCount{-1};
};

struct TopologyEdge {
  TopologyNode from;
  TopologyNode to;
};

struct TopologyPrinter {
  virtual void printNode(const TopologyNode& node) {
    static_cast<void>(node);
  }
  virtual void printLink(const TopologyNode& from, const TopologyNode& to) = 0;

  template <typename T>
  TopologyNode makeNode(T& stream) {
    using Value = typename decltype(ValueOf(stream))::type;
    const StreamBase* base;
    if constexpr (std::is_base_of_v<StreamBase, std::remove_cvref_t<T>>) {
      base = &static_cast<const StreamBase&>(stream);
    } else {
      base = &stream.getBase();
    }
    std::string typeName{StreamBuilderContext::getType<Value>()};
    if (const auto separator = typeName.rfind("::");
        separator != std::string::npos) {
      typeName.erase(0, separator + 2);
    }
    std::int64_t emittedCount = -1;
    if constexpr (requires { stream.getTopologyCount(); }) {
      emittedCount = stream.getTopologyCount();
    }
    return TopologyNode{stream.getId(), base->getConfigId(), stream.getName(),
                        std::move(typeName), emittedCount};
  }

 private:
  template <typename T>
    requires requires { typename std::remove_cvref_t<T>::topology_value_type; }
  static std::type_identity<
      typename std::remove_cvref_t<T>::topology_value_type>
  ValueOf(const T&);

  template <typename Value>
  static std::type_identity<Value> ValueOf(const StreamConsumer<Value>&);
};

struct StatusTopologyPrinter final : TopologyPrinter {
  std::vector<TopologyNode> nodes;
  std::vector<TopologyEdge> edges;

  void printNode(const TopologyNode& node) override { AddNode(node); }

  void printLink(const TopologyNode& from, const TopologyNode& to) override {
    AddNode(from);
    AddNode(to);
    edges.push_back({ResolveNode(from), ResolveNode(to)});
  }

 private:
  TopologyNode ResolveNode(const TopologyNode& node) const {
    const auto existing =
        std::ranges::find(nodes, node.id, &TopologyNode::id);
    return existing == nodes.end() ? node : *existing;
  }

  void AddNode(const TopologyNode& node) {
    const auto existing =
        std::ranges::find(nodes, node.id, &TopologyNode::id);
    if (existing == nodes.end()) {
      nodes.push_back(node);
    } else {
      existing->configId = node.configId;
      existing->name = node.name;
      existing->typeName = node.typeName;
      if (existing->emittedCount < 0 && node.emittedCount >= 0) {
        existing->emittedCount = node.emittedCount;
      }
    }
  }
};

struct VisTopologyPrinter : public TopologyPrinter {
  std::stringstream nodes;
  std::stringstream edges;
  std::unordered_set<size_t> added;

  void printNode(const TopologyNode& node) override {
    if (added.find(node.id) == added.end()) {
      nodes << "{ id: " << node.id << ", label: \"" << node.name << "\" },"
            << std::endl;
      added.emplace(node.id);
    }
  }

  void printLink(const TopologyNode& from, const TopologyNode& to) override {
    printNode(from);
    printNode(to);
    edges << "{ from: " << from.id << ", to: " << to.id << ", arrows:'to' },"
          << std::endl;
  }

  std::string getNodes() const { return nodes.str(); }

  std::string getEdges() const { return edges.str(); }
};

}  // namespace servicelib
