local function is_unsigned_integer(value)
    return type(value) == 'string'
        and (value == '0' or string.match(value, '^[1-9][0-9]*$') ~= nil)
end

local function compare_unsigned_integers(left, right)
    if string.len(left) ~= string.len(right) then
        return string.len(left) < string.len(right) and -1 or 1
    end
    if left == right then
        return 0
    end
    return left < right and -1 or 1
end

local function is_timestamp(value)
    return type(value) == 'string'
        and string.match(value, '^%d%d%d%d%-%d%d%-%d%dT%d%d:%d%d:%d%d%.%d%d%d%d%d%dZ$') ~= nil
end

local function is_uuid(value)
    return type(value) == 'string'
        and string.match(value, '^[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]%-[0-9a-f][0-9a-f][0-9a-f][0-9a-f]%-[0-9a-f][0-9a-f][0-9a-f][0-9a-f]%-[0-9a-f][0-9a-f][0-9a-f][0-9a-f]%-[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]$') ~= nil
end

local function is_finite_number(value)
    local number = tonumber(value)
    return number ~= nil and number == number
        and number ~= math.huge and number ~= -math.huge
end

local function is_vehicle_state(value)
    return value == 'REGISTERED'
        or value == 'ACTIVE'
        or value == 'IDLE'
        or value == 'MAINTENANCE'
        or value == 'DECOMMISSIONED'
end

local function key_type(key)
    return redis.call('TYPE', key).ok
end

local vehicle_key = KEYS[1]
local progress_key = KEYS[2]
local operation = ARGV[1]
local next_offset = ARGV[2]
local progress_field = ARGV[3]

if not is_unsigned_integer(next_offset) or next_offset == '0' then
    return redis.error_reply('invalid next Kafka offset')
end
if type(progress_field) ~= 'string' or string.len(progress_field) == 0 then
    return redis.error_reply('invalid progress field')
end

local vehicle_type = key_type(vehicle_key)
local progress_type = key_type(progress_key)
if vehicle_type ~= 'none' and vehicle_type ~= 'hash' then
    return redis.error_reply('vehicle projection key is not a hash')
end
if progress_type ~= 'none' and progress_type ~= 'hash' then
    return redis.error_reply('projection progress key is not a hash')
end

local current_offset = redis.call('HGET', progress_key, progress_field)
if current_offset and not is_unsigned_integer(current_offset) then
    return redis.error_reply('stored projection offset is invalid')
end

local vehicle_seen = nil
local is_deleted = nil
local state_present = nil
local position_seen = nil
local current_version = nil
local current_state = nil
local current_state_updated_at = nil
local current_position_id = nil
local current_recorded_at = nil
local current_latitude = nil
local current_longitude = nil

if vehicle_type == 'hash' then
    vehicle_seen = redis.call('HGET', vehicle_key, 'vehicle_seen')
    is_deleted = redis.call('HGET', vehicle_key, 'is_deleted')
    state_present = redis.call('HGET', vehicle_key, 'state_present')
    position_seen = redis.call('HGET', vehicle_key, 'position_seen')
    if (vehicle_seen ~= '0' and vehicle_seen ~= '1')
        or (is_deleted ~= '0' and is_deleted ~= '1')
        or (state_present ~= '0' and state_present ~= '1')
        or (position_seen ~= '0' and position_seen ~= '1') then
        return redis.error_reply('vehicle projection markers are invalid')
    end

    if is_deleted == '1' then
        if vehicle_seen ~= '1' or state_present ~= '0' or position_seen ~= '0' then
            return redis.error_reply('deleted vehicle projection markers are inconsistent')
        end
        local delete_version = redis.call('HGET', vehicle_key, 'delete_state_version')
        if not delete_version or not is_unsigned_integer(delete_version) or delete_version == '0' then
            return redis.error_reply('deleted vehicle version is invalid')
        end
    else
        if redis.call('HEXISTS', vehicle_key, 'delete_state_version') == 1 then
            return redis.error_reply('active vehicle has deletion metadata')
        end
        if state_present == '1' then
            current_version = redis.call('HGET', vehicle_key, 'state_version')
            current_state = redis.call('HGET', vehicle_key, 'state')
            current_state_updated_at = redis.call('HGET', vehicle_key, 'state_updated_at')
            if not current_version or not is_unsigned_integer(current_version) or current_version == '0'
                or not current_state or not current_state_updated_at
                or not is_timestamp(current_state_updated_at) or vehicle_seen ~= '1' then
                return redis.error_reply('vehicle state projection is inconsistent')
            end
        elseif redis.call('HEXISTS', vehicle_key, 'state_version') == 1
            or redis.call('HEXISTS', vehicle_key, 'state') == 1
            or redis.call('HEXISTS', vehicle_key, 'state_updated_at') == 1 then
            return redis.error_reply('vehicle state fields exist without a state marker')
        end

        if position_seen == '1' then
            current_position_id = redis.call('HGET', vehicle_key, 'position_id')
            current_recorded_at = redis.call('HGET', vehicle_key, 'recorded_at')
            current_latitude = redis.call('HGET', vehicle_key, 'lat')
            current_longitude = redis.call('HGET', vehicle_key, 'lng')
            if not current_position_id or not is_uuid(current_position_id)
                or not current_recorded_at or not is_timestamp(current_recorded_at)
                or not current_latitude or not is_finite_number(current_latitude)
                or not current_longitude or not is_finite_number(current_longitude) then
                return redis.error_reply('vehicle position projection is inconsistent')
            end
        elseif redis.call('HEXISTS', vehicle_key, 'position_id') == 1
            or redis.call('HEXISTS', vehicle_key, 'recorded_at') == 1
            or redis.call('HEXISTS', vehicle_key, 'lat') == 1
            or redis.call('HEXISTS', vehicle_key, 'lng') == 1 then
            return redis.error_reply('vehicle position fields exist without a position marker')
        end
    end
end

local status = 0

if operation == 'vehicle' then
    local incoming_version = ARGV[4]
    local incoming_state = ARGV[5]
    local incoming_updated_at = ARGV[6]
    if not is_unsigned_integer(incoming_version) or incoming_version == '0'
        or not is_vehicle_state(incoming_state)
        or not is_timestamp(incoming_updated_at) then
        return redis.error_reply('invalid vehicle state candidate')
    end

    if is_deleted == '1' then
        status = 4
    elseif current_version then
        local order = compare_unsigned_integers(incoming_version, current_version)
        if order < 0 then
            status = 0
        elseif order == 0 then
            if incoming_state ~= current_state or incoming_updated_at ~= current_state_updated_at then
                return redis.error_reply('conflicting vehicle payload for the same state version')
            end
            status = 0
        else
            redis.call(
                'HSET', vehicle_key,
                'vehicle_seen', '1',
                'is_deleted', '0',
                'state_present', '1',
                'position_seen', position_seen or '0',
                'state', incoming_state,
                'state_version', incoming_version,
                'state_updated_at', incoming_updated_at)
            status = 1
        end
    else
        redis.call(
            'HSET', vehicle_key,
            'vehicle_seen', '1',
            'is_deleted', '0',
            'state_present', '1',
            'position_seen', position_seen or '0',
            'state', incoming_state,
            'state_version', incoming_version,
            'state_updated_at', incoming_updated_at)
        status = 1
    end
elseif operation == 'position' then
    local incoming_position_id = ARGV[4]
    local incoming_recorded_at = ARGV[5]
    local incoming_latitude = ARGV[6]
    local incoming_longitude = ARGV[7]
    local latitude = tonumber(incoming_latitude)
    local longitude = tonumber(incoming_longitude)
    if not is_uuid(incoming_position_id) or not is_timestamp(incoming_recorded_at)
        or not is_finite_number(incoming_latitude) or latitude < -90 or latitude > 90
        or not is_finite_number(incoming_longitude) or longitude < -180 or longitude > 180 then
        return redis.error_reply('invalid position candidate')
    end

    if is_deleted == '1' then
        status = 4
    elseif position_seen == '1' then
        local newer = incoming_recorded_at > current_recorded_at
            or (incoming_recorded_at == current_recorded_at and incoming_position_id > current_position_id)
        local same_tuple = incoming_recorded_at == current_recorded_at
            and incoming_position_id == current_position_id
        if same_tuple then
            if incoming_latitude ~= current_latitude or incoming_longitude ~= current_longitude then
                return redis.error_reply('conflicting position payload for the same ordering tuple')
            end
            status = 0
        elseif newer then
            redis.call(
                'HSET', vehicle_key,
                'position_seen', '1',
                'position_id', incoming_position_id,
                'lat', incoming_latitude,
                'lng', incoming_longitude,
                'recorded_at', incoming_recorded_at)
            status = 1
        else
            status = 0
        end
    else
        redis.call(
            'HSET', vehicle_key,
            'vehicle_seen', vehicle_seen or '0',
            'is_deleted', '0',
            'state_present', state_present or '0',
            'position_seen', '1',
            'position_id', incoming_position_id,
            'lat', incoming_latitude,
            'lng', incoming_longitude,
            'recorded_at', incoming_recorded_at)
        status = 1
    end
elseif operation == 'delete' then
    local delete_version = ARGV[4]
    if not is_unsigned_integer(delete_version) or delete_version == '0' then
        return redis.error_reply('invalid vehicle delete version')
    end

    if is_deleted == '1' then
        if redis.call('HGET', vehicle_key, 'delete_state_version') ~= delete_version then
            return redis.error_reply('conflicting vehicle delete before-image')
        end
        status = 0
    else
        redis.call(
            'HSET', vehicle_key,
            'vehicle_seen', '1',
            'is_deleted', '1',
            'state_present', '0',
            'position_seen', '0',
            'delete_state_version', delete_version)
        redis.call(
            'HDEL', vehicle_key,
            'state', 'state_version', 'state_updated_at',
            'position_id', 'lat', 'lng', 'recorded_at')
        status = 2
    end
elseif operation == 'tombstone' then
    status = 3
else
    return redis.error_reply('unsupported projection operation')
end

local existing_offset = redis.call('HGET', progress_key, progress_field)
if not existing_offset or compare_unsigned_integers(next_offset, existing_offset) > 0 then
    redis.call('HSET', progress_key, progress_field, next_offset)
end

return status
