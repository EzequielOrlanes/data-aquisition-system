#include <boost/asio.hpp>
#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <ctime>
#include <sstream>
#include <iomanip>
#include <thread>
using boost::asio::ip::tcp;
using namespace std;
// Estrutura para armazenar os dados dos sensores
#pragma pack(push, 1)
struct LogRecord {
    char sensor_id[32];
    time_t timestamp;
    double value;
};
#pragma pack(pop)
unordered_map<string, mutex> file_mutex_map;
// Funções auxiliares para converter entre time_t e string
time_t string_to_time_t(const string &time_string) {
    tm tm = {};
    istringstream ss(time_string);
    ss >> get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    return mktime(&tm);
}
string time_t_to_string(time_t time) {
    tm *tm = localtime(&time);
    ostringstream ss;
    ss << put_time(tm, "%Y-%m-%dT%H:%M:%S");
    return ss.str();
}
// Função auxiliar para verificar prefixo de string
bool starts_with(const string &str, const string &prefix) {
    return str.size() >= prefix.size() && str.compare(0, prefix.size(), prefix) == 0;
}
// Classe para gerenciar as conexões com os clientes
class SensorServer {
public:
    SensorServer(boost::asio::io_context &io_context, short port)
        : acceptor_(io_context, tcp::endpoint(tcp::v4(), port)) {
        start_accept();
    }
private:
    void start_accept() {
        auto new_socket = make_shared<tcp::socket>(acceptor_.get_executor().context());
        acceptor_.async_accept(*new_socket, [this, new_socket](const boost::system::error_code &ec) {
            if (!ec) {
                cout << "Nova conexão aceita.\n";
                handle_client(new_socket);
            }
            start_accept();
        });
    }
    void handle_client(shared_ptr<tcp::socket> socket) {
        auto buffer = make_shared<vector<char>>(1024);
        socket->async_read_some(boost::asio::buffer(*buffer), [this, socket, buffer](const boost::system::error_code &ec, size_t bytes_transferred) {
            if (!ec) {
                string message(buffer->data(), bytes_transferred);
                process_message(socket, message);
                handle_client(socket); // Continue lendo mensagens
            } else {
                cerr << "Erro na conexão: " << ec.message() << "\n";
            }
        });
    }
    void process_message(shared_ptr<tcp::socket> socket, const string &message) {
        if (starts_with(message, "LOG|")) {
            handle_log_message(message);
        } else if (starts_with(message, "GET|")) {
            handle_get_message(socket, message);
        } else {
            cerr << "Mensagem inválida recebida: " << message << "\n";
        }
    }
    void handle_log_message(const string &message) {
        istringstream iss(message);
        string token;
        getline(iss, token, '|'); // LOG
        getline(iss, token, '|'); // SENSOR_ID
        string sensor_id = token;

        getline(iss, token, '|'); // DATA_HORA
        time_t timestamp = string_to_time_t(token);

        getline(iss, token, '|'); // LEITURA
        double value = stod(token);
        LogRecord record;
        strncpy(record.sensor_id, sensor_id.c_str(), sizeof(record.sensor_id));
        record.timestamp = timestamp;
        record.value = value;

        lock_guard<mutex> lock(file_mutex_map[sensor_id]);
        ofstream ofs(sensor_id + ".log", ios::binary | ios::app);
        ofs.write(reinterpret_cast<const char *>(&record), sizeof(record));
    }

    void handle_get_message(shared_ptr<tcp::socket> socket, const string &message) {
        istringstream iss(message);
        string token;

        getline(iss, token, '|'); // GET
        getline(iss, token, '|'); // SENSOR_ID
        string sensor_id = token;
        getline(iss, token, '|'); // NUMERO_DE_REGISTROS
        int num_records = stoi(token);

        if (file_mutex_map.find(sensor_id) == file_mutex_map.end()) {
            send_message(socket, "ERROR|INVALID_SENSOR_ID\r\n");
            return;
        }
        lock_guard<mutex> lock(file_mutex_map[sensor_id]);
        ifstream ifs(sensor_id + ".log", ios::binary);
        ifs.seekg(0, ios::end);
        size_t file_size = ifs.tellg();

        size_t record_count = file_size / sizeof(LogRecord);
        size_t records_to_read = min(static_cast<size_t>(num_records), record_count);

        ifs.seekg(-(records_to_read * sizeof(LogRecord)), ios::end);
        vector<LogRecord> records(records_to_read);
        ifs.read(reinterpret_cast<char *>(records.data()), records_to_read * sizeof(LogRecord));

        ostringstream response;
        response << records_to_read;
        for (const auto &record : records) {
            response << ";" << time_t_to_string(record.timestamp) << "|" << record.value;
        }
        response << "\r\n";

        send_message(socket, response.str());
    }

    void send_message(shared_ptr<tcp::socket> socket, const string &message) {
        boost::asio::async_write(*socket, boost::asio::buffer(message), [](const boost::system::error_code &ec, size_t) {
            if (ec) {
                cerr << "Erro ao enviar mensagem: " << ec.message() << "\n";
            }
        });
    }

    tcp::acceptor acceptor_;
};

int main() {
    try {
        boost::asio::io_context io_context;
        SensorServer server(io_context, 9000);
        io_context.run();
    } catch (exception &e) {
        cerr << "Erro: " << e.what() << "\n";
    }

    return 0;
}
