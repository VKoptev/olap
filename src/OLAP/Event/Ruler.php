<?php

namespace OLAP\Event;

class Ruler {

    /**
     * @var Ruler
     */
    static private $instance = null;

    /**
     * @var array
     */
    private $listeners = [];

    protected function __construct() {

        // do nothing
    }

    /**
     * @return Ruler
     */
    static public function getInstance() {

        if (empty(self::$instance)) {
            self::$instance = new static();
        }
        return self::$instance;
    }

    public function addListener(Listener $callback) {

        $type = $callback->getType();
        $id = $callback->getId();
        if (!isset($this->listeners[$type])) {
            $this->listeners[$type] = [
                'id' => [],
                'global' => [],
            ];
        }
        if ($id) {
            if (!isset($this->listeners[$type]['id'][$id])) {
                $this->listeners[$type]['id'][$id] = [];
            }
            $this->listeners[$type]['id'][$id][] = $callback;
        } else {
            $this->listeners[$type]['global'][] = $callback;
        }
    }

    public function trigger($type, $id = null, $args = []) {

        if (!isset($this->listeners[$type])) {
            return;
        }
        foreach ($this->listeners[$type]['global'] as $listener) {
            $listener->run($args);
        }
        if (isset($this->listeners[$type]['id'][$id])) {
            foreach ($this->listeners[$type]['id'][$id] as $listener) {
                $listener->run($args);
            }
        }
    }
}