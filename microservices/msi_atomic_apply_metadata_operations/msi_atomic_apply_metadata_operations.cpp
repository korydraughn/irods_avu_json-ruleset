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
            throw SYS_INVALID_INPUT_PARAM;
        }

        return s;
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

            using irods::experimental::io::idstream;
            using irods::experimental::io::server::default_transport;

            default_transport tp{*_rei->rsComm};
            idstream in{tp, path};

            if (!in) {
                rodsLog(LOG_ERROR, "Could not open data object [%s] for reading.", path.c_str());
                return FILE_OPEN_ERR;
            }

            json_t* root;

            {
                // Read the data object's contents into memory.
                std::string json;
                std::copy(std::istream_iterator<char>{in}, std::istream_iterator<char>{}, std::begin(json));

                json_error_t error;
                root = json_loads(json.c_str(), 0, &error);

                if (!root || !json_is_object(root)) {
                    rodsLog(LOG_ERROR, "Could not parse string into JSON [json_parse_error => %s].", error.text);
                    return SYS_INVALID_INPUT_PARAM;
                }
            }

            // Add the entity information to the JSON object.
            if (json_object_set(root, "entity_name", json_string(entity_name.c_str())) != 0 ||
                json_object_set(root, "entity_type", json_string(entity_type.c_str())) != 0)
            {
                rodsLog(LOG_ERROR, "Failed to add entity information to the JSON object.");
                return SYS_INTERNAL_ERR;
            }

            // Encode the JSON object into a string.
            auto* json_string = json_dumps(root, JSON_COMPACT);

            irods::at_scope_exit<std::function<void()>> free_memory{[json_string] {
                std::free(json_string);
            }};

            if (!json_string) {
                rodsLog(LOG_ERROR, "Could not encode JSON object into string.");
                return SYS_INTERNAL_ERR;
            }
            
            // TODO Atomically apply all metadata operations to the data object.
            //char* json_output;
            //return rc_atomic_apply_metadata_operations(_rei->rsComm, json_string, &json_output);
            return 0;
        }
        catch (const std::exception& e) {
            rodsLog(LOG_ERROR, e.what());
            return SYS_INTERNAL_ERR;
        }
        catch (const int error_code) {
            rodsLog(LOG_ERROR, "Could not convert microservice argument to string.");
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
irods::ms_table_entry* plugin_factory()
{
    const char* name = "msiAtomicApplyMetadataOperations";
    return make_msi<msParam_t*, msParam_t*, msParam_t*>(name, msi_impl);
}

