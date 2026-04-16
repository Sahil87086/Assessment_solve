#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <set>

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];

        if (arg == "--r_name") r_name = argv[++i];
        else if (arg == "--start_date") start_date = argv[++i];
        else if (arg == "--end_date") end_date = argv[++i];
        else if (arg == "--threads") num_threads = std::stoi(argv[++i]);
        else if (arg == "--table_path") table_path = argv[++i];
        else if (arg == "--result_path") result_path = argv[++i];
    }

    return !r_name.empty() && !start_date.empty() &&
           !end_date.empty() && num_threads > 0;
}

std::vector<std::map<std::string, std::string>>
readTable(const std::string& file_path) {

    std::vector<std::map<std::string, std::string>> data;
    std::ifstream file(file_path);
    std::string line;

    std::vector<std::string> headers;

    if (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string col;
        while (std::getline(ss, col, '|')) {
            headers.push_back(col);
        }
    }

    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string val;
        std::map<std::string, std::string> row;

        int i = 0;
        while (std::getline(ss, val, '|') && i < headers.size()) {
            row[headers[i++]] = val;
        }

        data.push_back(row);
    }

    return data;
}

bool readTPCHData(const std::string& table_path,
                  std::vector<std::map<std::string, std::string>>& customer_data,
                  std::vector<std::map<std::string, std::string>>& orders_data,
                  std::vector<std::map<std::string, std::string>>& lineitem_data,
                  std::vector<std::map<std::string, std::string>>& supplier_data,
                  std::vector<std::map<std::string, std::string>>& nation_data,
                  std::vector<std::map<std::string, std::string>>& region_data) {

    customer_data = readTable(table_path + "/customer.tbl");
    orders_data   = readTable(table_path + "/orders.tbl");
    lineitem_data = readTable(table_path + "/lineitem.tbl");
    supplier_data = readTable(table_path + "/supplier.tbl");
    nation_data   = readTable(table_path + "/nation.tbl");
    region_data   = readTable(table_path + "/region.tbl");

    return true;
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name,
                   const std::string& start_date,
                   const std::string& end_date,
                   int num_threads,
                   const std::vector<std::map<std::string, std::string>>& customer_data,
                   const std::vector<std::map<std::string, std::string>>& orders_data,
                   const std::vector<std::map<std::string, std::string>>& lineitem_data,
                   const std::vector<std::map<std::string, std::string>>& supplier_data,
                   const std::vector<std::map<std::string, std::string>>& nation_data,
                   const std::vector<std::map<std::string, std::string>>& region_data,
                   std::map<std::string, double>& results) {

    std::mutex mtx;

    // Build region filter
    std::set<std::string> valid_region_keys;
    for (auto& r : region_data) {
        if (r.at("r_name") == r_name)
            valid_region_keys.insert(r.at("r_regionkey"));
    }

    std::map<std::string, std::string> nation_region;
    for (auto& n : nation_data) {
        if (valid_region_keys.count(n.at("n_regionkey")))
            nation_region[n.at("n_nationkey")] = n.at("n_name");
    }

    auto worker = [&](int start, int end) {
        std::map<std::string, double> local_result;

        for (int i = start; i < end; i++) {
            const auto& o = orders_data[i];

            if (o.at("o_orderdate") < start_date ||
                o.at("o_orderdate") >= end_date)
                continue;

            std::string custkey = o.at("o_custkey");

            for (auto& c : customer_data) {
                if (c.at("c_custkey") != custkey) continue;

                std::string nationkey = c.at("c_nationkey");

                if (!nation_region.count(nationkey)) continue;

                std::string nation_name = nation_region[nationkey];

                for (auto& l : lineitem_data) {
                    if (l.at("l_orderkey") != o.at("o_orderkey")) continue;

                    double price = std::stod(l.at("l_extendedprice"));
                    double discount = std::stod(l.at("l_discount"));

                    local_result[nation_name] += price * (1 - discount);
                }
            }
        }

        std::lock_guard<std::mutex> lock(mtx);
        for (auto& [k, v] : local_result)
            results[k] += v;
    };

    int chunk = orders_data.size() / num_threads;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; i++) {
        int start = i * chunk;
        int end = (i == num_threads - 1) ? orders.size() : start + chunk;
        threads.emplace_back(worker, start, end);
    }

    for (auto& t : threads) t.join();

    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream out(result_path);

    for (auto& [nation, revenue] : results) {
        out << nation << " | " << revenue << "\n";
    }

    return true;
}