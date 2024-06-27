erDiagram
    SONGPLAYS {
        int songplay_id
        timestamp start_time
        int user_id
        varchar level
        varchar song_id
        varchar artist_id
        int session_id
        varchar location
        varchar user_agent
    }
    USERS {
        int user_id
        varchar first_name
        varchar last_name
        char gender
        varchar level
    }
    SONGS {
        varchar song_id
        varchar title
        varchar artist_id
        int year
        float duration
    }
    ARTISTS {
        varchar artist_id
        varchar name
        varchar location
        float latitude
        float longitude
    }
    TIME {
        timestamp start_time
        int hour
        int day
        int week
        int month
        int year
        varchar weekday
    }
    
    SONGPLAYS ||--|| USERS : "user_id"
    SONGPLAYS ||--|| SONGS : "song_id"
    SONGPLAYS ||--|| ARTISTS : "artist_id"
    SONGPLAYS ||--|| TIME : "start_time"
