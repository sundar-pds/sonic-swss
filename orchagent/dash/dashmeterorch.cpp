#include <swss/logger.h>
#include <swss/stringutility.h>
#include <swss/redisutility.h>
#include <swss/ipaddress.h>
#include <swssnet.h>

#include "dashmeterorch.h"
#include "taskworker.h"
#include "pbutils.h"
#include "crmorch.h"
#include "saihelper.h"

using namespace std;
using namespace swss;
using namespace dash::meter_policy;
using namespace dash::meter_rule;

extern sai_dash_meter_api_t* sai_dash_meter_api;
extern sai_object_id_t gSwitchId;
extern size_t gMaxBulkSize;
extern CrmOrch *gCrmOrch;

DashMeterOrch::DashMeterOrch(DBConnector *db, const vector<string> &tables, DashOrch *dash_orch, ZmqServer *zmqServer) :
    meter_rule_bulker_(sai_dash_meter_api, gSwitchId, gMaxBulkSize),
    ZmqOrch(db, tables, zmqServer),
    m_dash_orch(dash_orch)
{
    SWSS_LOG_ENTER();
}

bool DashMeterOrch::removeMeterRule(const string& key, MeterRuleBulkContext& ctxt)
{
    SWSS_LOG_ENTER();

    bool exists = (meter_rule_entries_.find(key) != meter_rule_entries_.end());
    if (!exists)
    {
        SWSS_LOG_WARN("Failed to find meter rule entry %s to remove", key.c_str());
        return true;
    }
    if (isMeterPolicyBound(ctxt.meter_policy))
    {
        SWSS_LOG_WARN("Cannot remove rule from meter policy %s as it is already bound", ctxt.meter_policy.c_str());
        return true;
    }

    auto& object_statuses = ctxt.object_statuses;
    object_statuses.emplace_back();
    meter_rule_bulker_.remove_entry(&object_statuses.back(), 
                                    meter_rule_entries_[key].meter_rule_oid);

    return false;
}

bool DashMeterOrch::removeMeterRulePost(const string& key, const MeterRuleBulkContext& ctxt)
{
    SWSS_LOG_ENTER();

    const auto& object_statuses = ctxt.object_statuses;
    if (object_statuses.empty())
    {
        return false;
    }

    auto it_status = object_statuses.begin();
    sai_status_t status = *it_status++;
    if (status != SAI_STATUS_SUCCESS)
    {
        // Retry later if object has non-zero reference to it
        if (status == SAI_STATUS_NOT_EXECUTED)
        {
            return false;
        }
        SWSS_LOG_ERROR("Failed to remove meter rule entry for %s", key.c_str());
        task_process_status handle_status = handleSaiRemoveStatus((sai_api_t) SAI_API_DASH_METER, status);
        if (handle_status != task_success)
        {
            return parseHandleSaiStatusFailure(handle_status);
        }
    }

    gCrmOrch->decCrmResUsedCounter(isV4(ctxt.meter_policy) ? CrmResourceType::CRM_DASH_IPV4_METER_RULE : CrmResourceType::CRM_DASH_IPV6_METER_RULE);
    meter_rule_entries_.erase(key);
    SWSS_LOG_INFO("Meter rule entry removed for %s", key.c_str());

    return true;
}

sai_object_id_t DashMeterOrch::getMeterPolicyOid(const string& meter_policy) const
{
    SWSS_LOG_ENTER();
    auto it = meter_policy_entries_.find(meter_policy);
    if (it == meter_policy_entries_.end())
    {
        return SAI_NULL_OBJECT_ID;
    }
    return it->second.meter_policy_oid;
}

uint32_t DashMeterOrch::getMeterPolicyRuleCount(const string& meter_policy) const
{
    SWSS_LOG_ENTER();
    auto it = meter_policy_entries_.find(meter_policy);
    if (it == meter_policy_entries_.end())
    {
        return 0;
    }
    return it->second.rule_count;
}

sai_ip_addr_family_t DashMeterOrch::getMeterPolicyAddrFamily(const string& meter_policy) const
{
    SWSS_LOG_ENTER();
    sai_ip_addr_family_t addr_family = SAI_IP_ADDR_FAMILY_IPV4;
    auto it = meter_policy_entries_.find(meter_policy);

    if (it != meter_policy_entries_.end())
    {
        to_sai(it->second.metadata.ip_version(), addr_family);
    }
    return addr_family;
}

bool DashMeterOrch::isV4(const string& meter_policy) const
{
    return (getMeterPolicyAddrFamily(meter_policy) == SAI_IP_ADDR_FAMILY_IPV4) ? true : false;
}

void DashMeterOrch::incrMeterPolicyRuleCount(const string& meter_policy)
{
    SWSS_LOG_ENTER();
    auto it = meter_policy_entries_.find(meter_policy);
    if (it != meter_policy_entries_.end())
    {
        it->second.rule_count += +1;
    }
    else
    {
        SWSS_LOG_WARN("Meter policy %s not found during rule count incr", meter_policy.c_str());
    }
}

void DashMeterOrch::decrMeterPolicyRuleCount(const string& meter_policy)
{
    SWSS_LOG_ENTER();
    auto it = meter_policy_entries_.find(meter_policy);
    if (it != meter_policy_entries_.end())
    {
        it->second.rule_count += -1;
        if (it->second.rule_count > 0)
        {
            it->second.rule_count += -1;
        }
        else
        {
            SWSS_LOG_WARN("Meter policy %s invalid rule count %d before decr", meter_policy.c_str(), it->second.rule_count);
        }
    }
    else
    {
        SWSS_LOG_WARN("Meter policy %s not found during rule count decr", meter_policy.c_str());
    }
}

int32_t DashMeterOrch::getMeterPolicyEniBindCount(const string& meter_policy) const
{
    SWSS_LOG_ENTER();
    auto it = meter_policy_entries_.find(meter_policy);
    if (it == meter_policy_entries_.end())
    {
        return 0;
    }
    return it->second.eni_bind_count;
}

void DashMeterOrch::incrMeterPolicyEniBindCount(const string& meter_policy)
{
    SWSS_LOG_ENTER();
    auto it = meter_policy_entries_.find(meter_policy);
    if (it != meter_policy_entries_.end())
    {
        it->second.eni_bind_count += 1;
        SWSS_LOG_INFO("Meter policy %s updated ENI bind count is %d", meter_policy.c_str(), 
                       getMeterPolicyEniBindCount(meter_policy));
    }
    else
    {
        SWSS_LOG_WARN("Meter policy %s not found during bind count incr", meter_policy.c_str());
    }
}

void DashMeterOrch::decrMeterPolicyEniBindCount(const string& meter_policy)
{
    SWSS_LOG_ENTER();
    auto it = meter_policy_entries_.find(meter_policy);
    if (it != meter_policy_entries_.end())
    {
        if (it->second.eni_bind_count > 0)
        {
            it->second.eni_bind_count += -1;
        }
        else
        {
            SWSS_LOG_WARN("Meter policy %s invalid bind count %d before decr", 
                           meter_policy.c_str(), it->second.eni_bind_count);
        }
        SWSS_LOG_INFO("Meter policy %s updated ENI bind count is %d", meter_policy.c_str(), 
                       getMeterPolicyEniBindCount(meter_policy));
    }
    else
    {
        SWSS_LOG_WARN("Meter policy %s not found during bind count decr", meter_policy.c_str());
    }
}

bool DashMeterOrch::isMeterPolicyBound(const std::string& meter_policy) const
{
    SWSS_LOG_ENTER();
    auto it = meter_policy_entries_.find(meter_policy);
    if (it == meter_policy_entries_.end())
    {
        return false;
    }
    return it->second.eni_bind_count > 0;
}

bool DashMeterOrch::addMeterRule(const string& key, MeterRuleBulkContext& ctxt)
{
    SWSS_LOG_ENTER();

    bool exists = (meter_rule_entries_.find(key) != meter_rule_entries_.end());
    if (exists)
    {
        SWSS_LOG_WARN("Meter rule entry already exists for %s", key.c_str());
        return true;
    }

    if (isMeterPolicyBound(ctxt.meter_policy))
    {
        SWSS_LOG_WARN("Cannot add new rule %s to Meter policy %s as it is already bound", key.c_str(), ctxt.meter_policy.c_str());
        return true;
    }

    sai_object_id_t meter_policy_oid = getMeterPolicyOid(ctxt.meter_policy);
    if (meter_policy_oid == SAI_NULL_OBJECT_ID)
    {
        SWSS_LOG_INFO("Retry for rule %s as meter policy %s not found", key.c_str(), ctxt.meter_policy.c_str());
        return false;
    }

    auto& object_ids = ctxt.object_ids;
    vector<sai_attribute_t> meter_rule_attrs;
    sai_attribute_t meter_rule_attr;

#if 1
    constexpr char *kDstIp1 = "10.206.196.31";
    const swss::IpAddress dst_ip = swss::IpAddress(kDstIp1);
    constexpr char *kDstMask1 = "255.255.255.0";
    const swss::IpAddress dst_mask = swss::IpAddress(kDstMask1);

    meter_rule_attr.id = SAI_METER_RULE_ATTR_DIP;
    swss::copy(meter_rule_attr.value.ipaddr, dst_ip);
    meter_rule_attrs.push_back(meter_rule_attr);

    meter_rule_attr.id = SAI_METER_RULE_ATTR_DIP_MASK;
    swss::copy(meter_rule_attr.value.ipaddr, dst_mask);
    meter_rule_attrs.push_back(meter_rule_attr);

    meter_rule_attr.id = SAI_METER_RULE_ATTR_IP_ADDR_FAMILY;
    meter_rule_attr.value.s32 = SAI_IP_ADDR_FAMILY_IPV4;
    meter_rule_attrs.push_back(meter_rule_attr);

#else

    meter_rule_attr.id = SAI_METER_RULE_ATTR_DIP;
    to_sai(ctxt.metadata.ip_prefix().ip(), meter_rule_attr.value.ipaddr);
    meter_rule_attrs.push_back(meter_rule_attr);

    meter_rule_attr.id = SAI_METER_RULE_ATTR_DIP_MASK;
    to_sai(ctxt.metadata.ip_prefix().mask(), meter_rule_attr.value.ipaddr);
    meter_rule_attrs.push_back(meter_rule_attr);

    meter_rule_attr.id = SAI_METER_RULE_ATTR_IP_ADDR_FAMILY;
    meter_rule_attr.value.s32 = getMeterPolicyAddrFamily(ctxt.meter_policy);
    meter_rule_attrs.push_back(meter_rule_attr);
#endif

    meter_rule_attr.id = SAI_METER_RULE_ATTR_METER_POLICY_ID;
    meter_rule_attr.value.oid = meter_policy_oid;
    meter_rule_attrs.push_back(meter_rule_attr);

    meter_rule_attr.id = SAI_METER_RULE_ATTR_METER_CLASS;
    meter_rule_attr.value.u32 = (uint32_t) ctxt.metadata.metering_class(); // TBD 64 to 32 bit conversion
    meter_rule_attrs.push_back(meter_rule_attr);

    meter_rule_attr.id = SAI_METER_RULE_ATTR_PRIORITY;
    meter_rule_attr.value.u32 = ctxt.metadata.priority();
    meter_rule_attrs.push_back(meter_rule_attr);

    object_ids.emplace_back();
    meter_rule_bulker_.create_entry(&object_ids.back(), (uint32_t)meter_rule_attrs.size(), meter_rule_attrs.data());

    return false;
}

bool DashMeterOrch::addMeterRulePost(const string& key, const MeterRuleBulkContext& ctxt)
{
    SWSS_LOG_ENTER();

    const auto& object_ids = ctxt.object_ids;
    if (object_ids.empty())
    {
        return false;
    }

    auto it_id = object_ids.begin();
    sai_object_id_t id = *it_id++;
    if (id == SAI_NULL_OBJECT_ID)
    {
        SWSS_LOG_ERROR("Failed to create meter rule entry for %s", key.c_str());
        return false;
    }

    meter_rule_entries_[key] = { id, ctxt.metadata, ctxt.meter_policy, ctxt.rule_num };
    incrMeterPolicyRuleCount(ctxt.meter_policy);

    gCrmOrch->incCrmResUsedCounter(isV4(ctxt.meter_policy) ? CrmResourceType::CRM_DASH_IPV4_METER_RULE : CrmResourceType::CRM_DASH_IPV6_METER_RULE);
    SWSS_LOG_INFO("Meter Rule entry for %s added", key.c_str());

    return true;
}

void DashMeterOrch::doTaskMeterRuleTable(ConsumerBase& consumer)
{
    SWSS_LOG_ENTER();

    auto it = consumer.m_toSync.begin();

    while (it != consumer.m_toSync.end())
    {
        std::map<std::pair<std::string, std::string>,
            MeterRuleBulkContext> toBulk;

        while (it != consumer.m_toSync.end())
        {
            KeyOpFieldsValuesTuple tuple = it->second;
            const string& key = kfvKey(tuple);
            auto op = kfvOp(tuple);
            auto rc = toBulk.emplace(std::piecewise_construct,
                    std::forward_as_tuple(key, op),
                    std::forward_as_tuple());
            bool inserted = rc.second;
            auto &ctxt = rc.first->second;

            if (!inserted)
            {
                ctxt.clear();
            }

            string& meter_policy = ctxt.meter_policy;
            uint32_t& rule_num   = ctxt.rule_num;

            vector<string> keys = tokenize(key, ':');
            meter_policy = keys[0];
            string rule_num_str;
            size_t pos = key.find(":", meter_policy.length());
            rule_num_str = key.substr(pos + 1);
            rule_num = stoi(rule_num_str);

#if 0
            string meter_policy; 
            string rule_num_str;

            if (!extractVariables(key, ':', meter_policy, rule_num_str))
            {
                SWSS_LOG_WARN("Failed to parse key %s", key.c_str());
                it = consumer.m_toSync.erase(it);
                continue;
                //return task_failed;
            }
            ctxt.meter_policy = meter_policy;
            ctxt.rule_num = stoi(rule_num_str);
#endif

            if (op == SET_COMMAND)
            {
                if (!parsePbMessage(kfvFieldsValues(tuple), ctxt.metadata))
                {
                    SWSS_LOG_WARN("Requires protobuff at MeterRule :%s", key.c_str());
                    it = consumer.m_toSync.erase(it);
                    continue;
                }
                if (addMeterRule(key, ctxt))
                {
                    it = consumer.m_toSync.erase(it);
                }
                else
                {
                    it++;
                }
            }
            else if (op == DEL_COMMAND)
            {
                if (removeMeterRule(key, ctxt))
                {
                    it = consumer.m_toSync.erase(it);
                }
                else
                {
                    it++;
                }
            }
            else
            {
                SWSS_LOG_ERROR("Unknown operation %s", op.c_str());
                it = consumer.m_toSync.erase(it);
            }
        }

        meter_rule_bulker_.flush();

        auto it_prev = consumer.m_toSync.begin();
        while (it_prev != it)
        {
            KeyOpFieldsValuesTuple t = it_prev->second;
            string key = kfvKey(t);
            string op = kfvOp(t);
            auto found = toBulk.find(make_pair(key, op));
            if (found == toBulk.end())
            {
                it_prev++;
                continue;
            }

            const auto& ctxt = found->second;
            const auto& object_statuses = ctxt.object_statuses;
            const auto& object_ids = ctxt.object_ids;

            if (op == SET_COMMAND)
            {
                if (object_ids.empty())
                {
                    it_prev++;
                    continue;
                }

                if (addMeterRulePost(key, ctxt))
                {
                    it_prev = consumer.m_toSync.erase(it_prev);
                }
                else
                {
                    it_prev++;
                }
            }
            else if (op == DEL_COMMAND)
            {
                if (object_statuses.empty())
                {
                    it_prev++;
                    continue;
                }

                if (removeMeterRulePost(key, ctxt))
                {
                    it_prev = consumer.m_toSync.erase(it_prev);
                }
                else
                {
                    it_prev++;
                }
            }
        }
    }
}

bool DashMeterOrch::addMeterPolicy(const string& meter_policy, MeterPolicyContext& ctxt)
{
    SWSS_LOG_ENTER();

    sai_object_id_t meter_policy_oid = getMeterPolicyOid(meter_policy);
    if (meter_policy_oid != SAI_NULL_OBJECT_ID)
    {
        SWSS_LOG_WARN("Meter policy %s already exists", meter_policy.c_str());
        return true;
    }

    vector<sai_attribute_t> meter_policy_attrs;
    sai_attribute_t meter_policy_attr;

    meter_policy_attr.id = SAI_METER_POLICY_ATTR_IP_ADDR_FAMILY;
    meter_policy_attr.value.u32 = ctxt.metadata.ip_version();
    meter_policy_attrs.push_back(meter_policy_attr);

    sai_status_t status = sai_dash_meter_api->create_meter_policy(&meter_policy_oid, gSwitchId, (uint32_t)meter_policy_attrs.size(), meter_policy_attrs.data());
    if (status != SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_ERROR("Failed to create Meter policy %s", meter_policy.c_str());
        task_process_status handle_status = handleSaiCreateStatus((sai_api_t) SAI_API_DASH_METER, status);
        if (handle_status != task_success)
        {
            return parseHandleSaiStatusFailure(handle_status);
        }
    }

    meter_policy_entries_[meter_policy] = { meter_policy_oid, ctxt.metadata, 0, 0};
    gCrmOrch->incCrmResUsedCounter(isV4(meter_policy) ? CrmResourceType::CRM_DASH_IPV4_METER_POLICY : CrmResourceType::CRM_DASH_IPV6_METER_POLICY);
    SWSS_LOG_INFO("Meter policy %s added", meter_policy.c_str());

    return true;
}

bool DashMeterOrch::removeMeterPolicy(const string& meter_policy)
{
    SWSS_LOG_ENTER();

    if (isMeterPolicyBound(meter_policy))
    {
        SWSS_LOG_WARN("Cannot remove bound meter policy %s", meter_policy.c_str());
        return false;
    }

    sai_object_id_t meter_policy_oid = getMeterPolicyOid(meter_policy);
    if (meter_policy_oid == SAI_NULL_OBJECT_ID)
    {
        SWSS_LOG_INFO("Failed to find meter policy %s to remove", meter_policy.c_str());
        return true;
    }

    uint32_t rule_count = getMeterPolicyRuleCount(meter_policy);
    if (rule_count != 0)
    {
        SWSS_LOG_INFO("Failed to remove meter policy %s due to rule count %d ", meter_policy.c_str(), rule_count);
        return true;
    }

    sai_status_t status = sai_dash_meter_api->remove_meter_policy(meter_policy_oid);
    if (status != SAI_STATUS_SUCCESS)
    {
        SWSS_LOG_ERROR("Failed to remove meter policy %s", meter_policy.c_str());
        task_process_status handle_status = handleSaiRemoveStatus((sai_api_t) SAI_API_DASH_METER, status);
        if (handle_status != task_success)
        {
            return parseHandleSaiStatusFailure(handle_status);
        }
    }

    meter_policy_entries_.erase(meter_policy);
    gCrmOrch->decCrmResUsedCounter(isV4(meter_policy) ? CrmResourceType::CRM_DASH_IPV4_METER_POLICY : CrmResourceType::CRM_DASH_IPV6_METER_POLICY);
    SWSS_LOG_INFO("Meter policy %s removed", meter_policy.c_str());

    return true;
}

void DashMeterOrch::doTaskMeterPolicyTable(ConsumerBase& consumer)
{
    SWSS_LOG_ENTER();

    auto it = consumer.m_toSync.begin();

    while (it != consumer.m_toSync.end())
    {
        auto tuple = it->second;
        auto op = kfvOp(tuple);
        const string& key = kfvKey(tuple);

        if (op == SET_COMMAND)
        {
            MeterPolicyContext ctxt;
            ctxt.meter_policy = key;

            if (!parsePbMessage(kfvFieldsValues(tuple), ctxt.metadata)) 
            {
                SWSS_LOG_WARN("Requires protobuff at MeterPolicy :%s", key.c_str());
                it = consumer.m_toSync.erase(it);
                continue;
            }
            if (addMeterPolicy(key, ctxt))
            {
                it = consumer.m_toSync.erase(it);
            }
            else
            {
                it++;
            }
        }
        else if (op == DEL_COMMAND)
        {
            if (removeMeterPolicy(key))
            {
                it = consumer.m_toSync.erase(it);
            }
            else
            {
                it++;
            }
        }
        else
        {
            SWSS_LOG_ERROR("Unknown operation %s", op.c_str());
            it = consumer.m_toSync.erase(it);
        }
    }
}

void DashMeterOrch::doTask(ConsumerBase& consumer)
{
    SWSS_LOG_ENTER();

    const auto& tn = consumer.getTableName();

    SWSS_LOG_INFO("Table name: %s", tn.c_str());

    if (tn == APP_DASH_METER_POLICY_TABLE_NAME)
    {
        doTaskMeterPolicyTable(consumer);
    }
    else if (tn == APP_DASH_METER_RULE_TABLE_NAME)
    {
        doTaskMeterRuleTable(consumer);
    }
    else
    {
        SWSS_LOG_ERROR("Unknown table: %s", tn.c_str());
    }
}