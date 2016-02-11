<?php

namespace OLAP\DB;

/**
 * Class Type
 * @package OLAP\DB
 * @method \OLAP\Type object()
 */
class Type extends Base {

    public function checkStructure() {

        $exists = $this->db()->fetchColumn("SELECT EXISTS (select 1 from pg_catalog.pg_type where typname = :typname)", [':typname' => $this->getTableName()]);
        $creation = $this->object()->getCreation();
        if (!$exists && $creation) {
            $this->db()->exec($creation);
        }
    }
}