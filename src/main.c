#include <include/connext.h>

static
void InitRtiConnextParticipant(ecs_rows_t *rows) {
    ECS_COLUMN(rows, DdsDomainParticipant, participant, 1);
    ECS_COLUMN_COMPONENT(rows, RtiConnextParticipant, 2);

    for (int i = 0; i < rows->count; i ++) {
        DDS_DomainParticipant *dp = DDS_DomainParticipantFactory_create_participant(
            DDS_TheParticipantFactory, participant[i].domain_id, 
            &DDS_PARTICIPANT_QOS_DEFAULT,
            NULL, 
            DDS_STATUS_MASK_NONE);

        if (!dp) { 
            fprintf(stderr, "failed to create participant for id %d\n", 
                participant[i].domain_id); 
            continue; 
        }

        ecs_set(rows->world, rows->entities[i], RtiConnextParticipant, {dp});
    }
}

static
void InitRtiConnextWriter(ecs_rows_t *rows) {
    ECS_SHARED(rows, RtiConnextParticipant, participant, 1);
    ECS_COLUMN(rows, RtiConnextTypeSupport, type_support, 2);
    ECS_COLUMN(rows, DdsWriter, writer, 3);
    ECS_COLUMN_COMPONENT(rows, RtiConnextWriter, 4);

    for (int i = 0; i < rows->count; i ++) {
        DDS_Publisher *pub = DDS_DomainParticipant_create_publisher(
            participant->dp, &DDS_PUBLISHER_QOS_DEFAULT, NULL, DDS_STATUS_MASK_NONE);
        if (pub == NULL) { 
            fprintf(stderr, "failed to create publisher\n"); 
            break; 
        };

        type_support[i].register_type(participant->dp, writer[i].type_name);

        DDS_Topic *topic = DDS_DomainParticipant_create_topic(
            participant->dp, writer[i].topic_name, writer[i].type_name, &DDS_TOPIC_QOS_DEFAULT, NULL,
            DDS_STATUS_MASK_NONE);
        if (topic == NULL) { 
            fprintf(stderr, "failed to create topic\n");
            break; 
        };

        DDS_DataWriter *dw = DDS_Publisher_create_datawriter(
            pub, topic, &DDS_DATAWRITER_QOS_DEFAULT, NULL,
            DDS_STATUS_MASK_NONE);
        if (dw == NULL) { 
            fprintf(stderr, "failed to create writer\n"); 
            break; 
        };

        ecs_set(rows->world, rows->entities[i], RtiConnextWriter, {
            .pub = pub,
            .topic = topic,
            .dw = dw
        });
    }
}

static
void InitRtiConnextReader(ecs_rows_t *rows) {
    ECS_SHARED(rows, RtiConnextParticipant, participant, 1);
    ECS_COLUMN(rows, RtiConnextTypeSupport, type_support, 2);
    ECS_COLUMN(rows, DdsReader, reader, 3);
    ECS_COLUMN_COMPONENT(rows, RtiConnextReader, 4);

    for (int i = 0; i < rows->count; i ++) {
        DDS_Subscriber *sub = DDS_DomainParticipant_create_subscriber(
            participant->dp, &DDS_SUBSCRIBER_QOS_DEFAULT, NULL, DDS_STATUS_MASK_NONE);
        if (sub == NULL) { 
            fprintf(stderr, "failed to create subscriber\n"); 
            break; 
        };

        type_support[i].register_type(participant->dp, reader[i].type_name);

        DDS_Topic *topic = DDS_DomainParticipant_create_topic(
            participant->dp, reader[i].topic_name, reader[i].type_name, &DDS_TOPIC_QOS_DEFAULT, NULL,
            DDS_STATUS_MASK_NONE);
        if (topic == NULL) { 
            fprintf(stderr, "failed to create topic\n");
            break; 
        };

        DDS_DataReader *dr = DDS_Subscriber_create_datareader(
            sub, DDS_Topic_as_topicdescription(topic), &DDS_DATAREADER_QOS_DEFAULT, NULL,
            DDS_STATUS_MASK_NONE);
        if (dr == NULL) { 
            fprintf(stderr, "failed to create reader\n"); 
            break; 
        };

        ecs_set(rows->world, rows->entities[i], RtiConnextReader, {
            .sub = sub,
            .topic = topic,
            .dr = dr
        });
    }
}

void RtiSystemsConnextImport(
    ecs_world_t *world,
    int flags)
{
    ECS_IMPORT(world, FlecsComponentsDds, 0);
    
    ECS_MODULE(world, RtiSystemsConnext);

    ECS_COMPONENT(world, RtiConnextParticipant);
    ECS_COMPONENT(world, RtiConnextTypeSupport);
    ECS_COMPONENT(world, RtiConnextReader);
    ECS_COMPONENT(world, RtiConnextWriter);

    ECS_SYSTEM(world, InitRtiConnextParticipant, EcsOnUpdate, DdsDomainParticipant, !RtiConnextParticipant);
    ECS_SYSTEM(world, InitRtiConnextWriter, EcsOnUpdate, 
        CONTAINER.RtiConnextParticipant, 
        RtiConnextTypeSupport, 
        DdsWriter, 
        !RtiConnextWriter);

    ECS_SYSTEM(world, InitRtiConnextReader, EcsOnUpdate, 
        CONTAINER.RtiConnextParticipant, 
        RtiConnextTypeSupport, 
        DdsReader, 
        !RtiConnextReader);

    ECS_SET_COMPONENT(handles, RtiConnextParticipant);
    ECS_SET_COMPONENT(handles, RtiConnextReader);
    ECS_SET_COMPONENT(handles, RtiConnextWriter);
}
