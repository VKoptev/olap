<?php

namespace OLAP\Event;


class Listener {

    private $callback;
    private $type;
    private $id;

    public function __construct($type, $callback, $id = null) {

        if (!Type::isValid($type)) {
            throw new Exception('Bad event type: ' . $type);
        }
        if (!is_callable($callback)) {
            throw new Exception('Is not callable listener');
        }
        $this->callback = $callback;
        $this->type = $type;
        $this->id = $id;
    }

    public function getType() {

        return $this->type;
    }

    public function getId() {

        return $this->id;
    }

    public function run(&$args) {

        call_user_func($this->callback, $args);
    }
}