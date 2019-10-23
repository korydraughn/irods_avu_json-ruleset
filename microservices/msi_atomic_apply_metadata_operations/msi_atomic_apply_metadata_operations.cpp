#include "irods_ms_plugin.hpp"
#include "irods_re_structs.hpp"
#include "msParam.h"
#include "rodsErrorTable.h"
#include "rodsLog.h"
#include "irods_at_scope_exit.hpp"
#include "dstream.hpp"

#define IRODS_IO_TRANSPORT_ENABLE_SERVER_SIDE_API
#include "transport/default_transport.hpp"

#include "jansson.h"

#include <functional>
#include <string>
#include <tuple>
#include <iterator>
#include <algorithm>
#include <exception>

namespace
{
    auto to_string(msParam_t* _p) -> std::string
    {
        const auto* s = parseMspForStr(_p);

        if (!s) {
            throw static_cast<int>(SYS_INVALID_INPUT_PARAM);
        }

        return s;
    }

    auto read_data_object(rsComm_t& _comm, const std::string& _path) -> std::string
    {
        using irods::experimental::io::idstream;
        using irods::experimental::io::server::default_transport;

        default_transport tp{_comm};
        idstream in{tp, _path};

        if (!in) {
            rodsLog(LOG_ERROR, "Failed to open data object for reading [data object => %s].", _path.c_str());
            throw static_cast<int>(FILE_OPEN_ERR);
        }

        std::string json;
        std::copy(std::istream_iterator<char>{in}, std::istream_iterator<char>{}, std::back_inserter(json));

        return json;
    }

    auto msi_impl(msParam_t* _entity_name,
                  msParam_t* _entity_type,
                  msParam_t* _path_to_data_object,
                  ruleExecInfo_t* _rei) -> int
    {
        try {
            const auto entity_name = to_string(_entity_name);
            const auto entity_type = to_string(_entity_type);
            const auto path = to_string(_path_to_data_object);

            json_t* root = nullptr;

            irods::at_scope_exit<std::function<void()>> free_json_root{[root] {
                json_decref(root);
            }};

            {
                const auto json = read_data_object(*_rei->rsComm, path);

                json_error_t error;
                root = json_loads(json.c_str(), 0, &error);

                if (!root || !json_is_object(root)) {
                    rodsLog(LOG_ERROR, "Failed to parse string into JSON [json parse error => %s].", error.text);
                    return SYS_INVALID_INPUT_PARAM;
                }
            }

            // Add the entity information to the JSON object.
            if (json_object_set_new(root, "entity_name", json_string(entity_name.c_str())) != 0 ||
                json_object_set_new(root, "entity_type", json_string(entity_type.c_str())) != 0)
            {
                rodsLog(LOG_ERROR, "Failed to add entity information to the JSON object.");
                return SYS_INTERNAL_ERR;
            }

            // Encode the JSON object into a string.
            //auto* json_string = json_dumps(root, JSON_COMPACT);
            auto* json_string = json_dumps(root, JSON_INDENT(4));

            irods::at_scope_exit<std::function<void()>> free_json_string{[json_string] {
                std::free(json_string);
            }};

            if (!json_string) {
                rodsLog(LOG_ERROR, "Failed to encode JSON object as string.");
                return SYS_INTERNAL_ERR;
            }

            // TODO Convert JSON to AVU metadata using converter.

#if 0
            // TODO Atomically apply all metadata operations to the data object.
            char* json_output;
            return rc_atomic_apply_metadata_operations(_rei->rsComm, json_string, &json_output);
#else
            rodsLog(LOG_NOTICE, "JSON DATA => %s", json_string);

            return 0;
#endif
        }
        catch (const std::exception& e) {
            rodsLog(LOG_ERROR, e.what());
            return SYS_INTERNAL_ERR;
        }
        catch (const int error_code) {
            rodsLog(LOG_ERROR, "Failed to convert microservice argument to string.");
            return error_code;
        }
        catch (...) {
            rodsLog(LOG_ERROR, "An unknown error occurred while processing the request.");
            return SYS_UNKNOWN_ERROR;
        }
    }

    template <typename... Args, typename Function>
    auto make_msi(const std::string& _name, Function _func) -> irods::ms_table_entry*
    {
        auto* msi = new irods::ms_table_entry{sizeof...(Args)};
        msi->add_operation<Args..., ruleExecInfo_t*>(_name, std::function<int(Args..., ruleExecInfo_t*)>(_func));
        return msi;
    }
} // anonymous namespace

extern "C"
auto plugin_factory() -> irods::ms_table_entry*
{
    const char* name = "msiAtomicApplyMetadataOperations";
    return make_msi<msParam_t*, msParam_t*, msParam_t*>(name, msi_impl);
}

