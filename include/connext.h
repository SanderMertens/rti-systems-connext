#ifndef FLECS_SYSTEMS_RTI_CONNEXT_H
#define FLECS_SYSTEMS_RTI_CONNEXT_H

/* This generated file contains includes for project dependencies */
#include "bake_config.h"
#include "ndds_c.h"

typedef struct RtiConnextParticipant {
    DDS_DomainParticipant *dp;
} RtiConnextParticipant;

typedef struct RtiConnextTypeSupport {
    DDS_ReturnCode_t (*register_type)(
        DDS_DomainParticipant *dp, 
        const char *type_name);
} RtiConnextTypeSupport;

typedef struct RtiConnextWriter {
    DDS_Publisher *pub;
    DDS_Topic *topic;
    DDS_DataWriter *dw;
} RtiConnextWriter;

typedef struct RtiConnextReader {
    DDS_Subscriber *sub;
    DDS_Topic *topic;
    DDS_DataReader *dr;
} RtiConnextReader;

typedef struct EcsSystemsRtiConnextHandles {
    ECS_DECLARE_COMPONENT(RtiConnextParticipant);
    ECS_DECLARE_COMPONENT(RtiConnextTypeSupport);
    ECS_DECLARE_COMPONENT(RtiConnextWriter);
    ECS_DECLARE_COMPONENT(RtiConnextReader);
} EcsSystemsRtiConnextHandles;

void EcsSystemsRtiConnext(
    ecs_world_t *world,
    int flags,
    void *handles_out);

#define EcsSystemsRtiConnext_ImportHandles(handles)\
    ECS_IMPORT_COMPONENT(handles, RtiConnextParticipant);\
    ECS_IMPORT_COMPONENT(handles, RtiConnextTypeSupport);\
    ECS_IMPORT_COMPONENT(handles, RtiConnextWriter);\
    ECS_IMPORT_COMPONENT(handles, RtiConnextReader);

#endif

